package topology

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/nethalo/dbsafe/internal/mysql"
)

// Type represents the detected MySQL topology.
type Type string

const (
	Standalone      Type = "standalone"
	AsyncReplica    Type = "async-replica"
	SemiSyncReplica Type = "semisync-replica"
	Galera          Type = "galera"
	GroupRepl       Type = "group-replication"
)

// Info holds the full topology state.
type Info struct {
	Type    Type
	Version mysql.ServerVersion

	// Replication (async/semisync)
	IsReplica      bool
	IsPrimary      bool // has replicas attached
	ReplicaLagSecs *int64

	// Galera / PXC
	GaleraClusterSize    int
	GaleraNodeState      string // Synced, Donor, Desynced, etc.
	GaleraOSUMethod      string // TOI or RSU
	WsrepMaxWsSize       int64  // bytes
	FlowControlPaused    float64
	FlowControlPausedPct string

	// Group Replication
	GRMode              string // SINGLE-PRIMARY or MULTI-PRIMARY
	GRMemberCount       int
	GRTransactionLimit  int64
	GRMemberRole        string // PRIMARY or SECONDARY

	// General
	ReadOnly    bool
	SuperReadOnly bool
}

// Detect connects to MySQL and determines the topology.
func Detect(db *sql.DB) (*Info, error) {
	info := &Info{}

	// Get version first
	version, err := mysql.GetServerVersion(db)
	if err != nil {
		return nil, err
	}
	info.Version = version

	// Check read_only
	ro, _ := mysql.GetVariable(db, "read_only")
	info.ReadOnly = ro == "ON"
	sro, _ := mysql.GetVariable(db, "super_read_only")
	info.SuperReadOnly = sro == "ON"

	// Try Galera detection first (most specific)
	if detected, err := detectGalera(db, info); err == nil && detected {
		return info, nil
	}

	// Try Group Replication
	if detected, err := detectGroupReplication(db, info); err == nil && detected {
		return info, nil
	}

	// Try async/semisync replication
	if detected, err := detectReplication(db, info); err == nil && detected {
		return info, nil
	}

	// Default: standalone
	info.Type = Standalone
	return info, nil
}

func detectGalera(db *sql.DB, info *Info) (bool, error) {
	// Check if wsrep provider is loaded
	clusterSize, err := mysql.GetVariable(db, "wsrep_cluster_size")
	if err != nil || clusterSize == "" {
		return false, nil
	}

	size, _ := strconv.Atoi(clusterSize)
	if size == 0 {
		return false, nil
	}

	info.Type = Galera
	info.GaleraClusterSize = size

	// Node state
	state, _ := mysql.GetStatus(db, "wsrep_local_state_comment")
	info.GaleraNodeState = state

	// OSU method
	osu, _ := mysql.GetVariable(db, "wsrep_OSU_method")
	info.GaleraOSUMethod = osu

	// Max write-set size
	maxWs, _ := mysql.GetVariableInt(db, "wsrep_max_ws_size")
	info.WsrepMaxWsSize = maxWs

	// Flow control
	fcPaused, _ := mysql.GetStatus(db, "wsrep_flow_control_paused")
	if fcPaused != "" {
		info.FlowControlPaused, _ = strconv.ParseFloat(fcPaused, 64)
		info.FlowControlPausedPct = fmt.Sprintf("%.2f%%", info.FlowControlPaused*100)
	}

	return true, nil
}

func detectGroupReplication(db *sql.DB, info *Info) (bool, error) {
	// Check if GR plugin is active
	grEnabled, err := mysql.GetVariable(db, "group_replication_group_name")
	if err != nil || grEnabled == "" {
		return false, nil
	}

	info.Type = GroupRepl

	// Single-primary vs multi-primary
	singlePrimary, _ := mysql.GetVariable(db, "group_replication_single_primary_mode")
	if singlePrimary == "ON" {
		info.GRMode = "SINGLE-PRIMARY"
	} else {
		info.GRMode = "MULTI-PRIMARY"
	}

	// Transaction size limit
	txLimit, _ := mysql.GetVariableInt(db, "group_replication_transaction_size_limit")
	info.GRTransactionLimit = txLimit

	// Member count and role from performance_schema
	rows, err := db.Query(`
		SELECT MEMBER_ROLE, MEMBER_STATE 
		FROM performance_schema.replication_group_members
		WHERE MEMBER_HOST = @@hostname AND MEMBER_PORT = @@port
	`)
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			var role, state string
			rows.Scan(&role, &state)
			info.GRMemberRole = role
		}
	}

	// Total member count
	var count int
	err = db.QueryRow(`
		SELECT COUNT(*) 
		FROM performance_schema.replication_group_members
		WHERE MEMBER_STATE = 'ONLINE'
	`).Scan(&count)
	if err == nil {
		info.GRMemberCount = count
	}

	return true, nil
}

func detectReplication(db *sql.DB, info *Info) (bool, error) {
	detected := false

	// Check if this server is a replica
	rows, err := db.Query("SHOW REPLICA STATUS")
	if err != nil {
		// Try older syntax
		rows, err = db.Query("SHOW SLAVE STATUS")
	}
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			info.IsReplica = true
			detected = true

			// Get column names to find Seconds_Behind_Source/Master
			cols, _ := rows.Columns()
			values := make([]sql.NullString, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			rows.Scan(ptrs...)

			for i, col := range cols {
				switch col {
				case "Seconds_Behind_Source", "Seconds_Behind_Master":
					if values[i].Valid {
						lag, _ := strconv.ParseInt(values[i].String, 10, 64)
						info.ReplicaLagSecs = &lag
					}
				}
			}
		}
	}

	// Check if this server has replicas (is a primary)
	var replCount int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND = 'Binlog Dump' OR COMMAND = 'Binlog Dump GTID'").Scan(&replCount)
	if err == nil && replCount > 0 {
		info.IsPrimary = true
		detected = true
	}

	if detected {
		// Check semi-sync
		semiSync, _ := mysql.GetVariable(db, "rpl_semi_sync_source_enabled")
		if semiSync == "" {
			semiSync, _ = mysql.GetVariable(db, "rpl_semi_sync_master_enabled")
		}
		if semiSync == "ON" {
			info.Type = SemiSyncReplica
		} else {
			info.Type = AsyncReplica
		}
	}

	return detected, nil
}
