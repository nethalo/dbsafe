package topology

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

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
	AuroraWriter    Type = "aurora-writer"
	AuroraReader    Type = "aurora-reader"
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
	GRMode             string // SINGLE-PRIMARY or MULTI-PRIMARY
	GRMemberCount      int
	GRTransactionLimit int64
	GRMemberRole       string // PRIMARY or SECONDARY

	// General
	ReadOnly      bool
	SuperReadOnly bool

	// Cloud
	IsCloudManaged bool
	CloudProvider  string // "aws-aurora", "aws-rds", ""
}

// Detect connects to MySQL and determines the topology.
// Set verbose to true to enable debug logging.
func Detect(db *sql.DB, verbose bool) (*Info, error) {
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

	// Aurora detection: must happen before Galera/GR since Aurora has its own replication model.
	if version.IsAurora() {
		info.IsCloudManaged = true
		info.CloudProvider = "aws-aurora"
		if info.ReadOnly {
			info.Type = AuroraReader
		} else {
			info.Type = AuroraWriter
		}
		return info, nil
	}

	// Try Galera detection first (most specific)
	detected, err := detectGalera(db, info, verbose)
	if err != nil {
		return nil, fmt.Errorf("galera detection failed: %w", err)
	}
	if detected {
		return info, nil
	}

	// Try Group Replication
	detected, err = detectGroupReplication(db, info)
	if err != nil {
		return nil, fmt.Errorf("group replication detection failed: %w", err)
	}
	if detected {
		return info, nil
	}

	// Try async/semisync replication
	detected, err = detectReplication(db, info)
	if err != nil {
		return nil, fmt.Errorf("replication detection failed: %w", err)
	}
	if detected {
		return info, nil
	}

	// Default: standalone
	info.Type = Standalone

	// RDS detection (best-effort): check basedir for rdsdbbin marker.
	// This annotates without changing topology type — RDS uses standard MySQL replication.
	basedir, _ := mysql.GetVariable(db, "basedir")
	if strings.Contains(basedir, "rdsdbbin") {
		info.IsCloudManaged = true
		info.CloudProvider = "aws-rds"
	}

	return info, nil
}

func detectGalera(db *sql.DB, info *Info, verbose bool) (bool, error) {
	// First, check if this is PXC by looking at version_comment
	versionComment, _ := mysql.GetVariable(db, "version_comment")
	if verbose {
		log.Printf("[DEBUG] version_comment: %q", versionComment)
	}

	// Check if wsrep is enabled
	wsrepOn, err := mysql.GetVariable(db, "wsrep_on")
	if verbose {
		log.Printf("[DEBUG] GetVariable('wsrep_on') returned: value=%q, err=%v", wsrepOn, err)
		log.Printf("[DEBUG] wsrepOn length: %d, wsrepOn bytes: %v", len(wsrepOn), []byte(wsrepOn))
	}
	if err != nil {
		// Actual error (not just variable doesn't exist)
		return false, fmt.Errorf("failed to get wsrep_on: %w", err)
	}
	if wsrepOn != "ON" {
		if verbose {
			log.Printf("[DEBUG] wsrep_on is not 'ON' (got %q), not a Galera cluster", wsrepOn)
		}
		return false, nil
	}

	// Check cluster size from status (more reliable than variable)
	clusterSize, err := mysql.GetStatus(db, "wsrep_cluster_size")
	if verbose {
		log.Printf("[DEBUG] GetStatus('wsrep_cluster_size') returned: value=%q, err=%v", clusterSize, err)
	}
	if err != nil || clusterSize == "" {
		// Status variable doesn't exist or is empty, fallback to variable
		clusterSize, err = mysql.GetVariable(db, "wsrep_cluster_size")
		if verbose {
			log.Printf("[DEBUG] Fallback GetVariable('wsrep_cluster_size') returned: value=%q, err=%v", clusterSize, err)
		}
		if err != nil {
			return false, fmt.Errorf("failed to get wsrep_cluster_size: %w", err)
		}
		if clusterSize == "" {
			if verbose {
				log.Printf("[DEBUG] wsrep_cluster_size is empty, not a Galera cluster")
			}
			return false, nil
		}
	}

	size, err := strconv.Atoi(clusterSize)
	if err != nil {
		return false, fmt.Errorf("invalid wsrep_cluster_size value '%s': %w", clusterSize, err)
	}
	if size == 0 {
		if verbose {
			log.Printf("[DEBUG] wsrep_cluster_size is 0, not a Galera cluster")
		}
		return false, nil
	}

	if verbose {
		log.Printf("[DEBUG] Galera/PXC detected! Cluster size: %d", size)
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
			if err := rows.Scan(&role, &state); err == nil {
				info.GRMemberRole = role
			}
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
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return false, fmt.Errorf("scanning replica status: %w", err)
			}

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
