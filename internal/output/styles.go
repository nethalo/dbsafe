package output

import (
	"github.com/charmbracelet/lipgloss"
)

// Colors
var (
	ColorSafe    = lipgloss.Color("#04B575") // green
	ColorWarning = lipgloss.Color("#FFB800") // yellow
	ColorDanger  = lipgloss.Color("#FF4040") // red
	ColorInfo    = lipgloss.Color("#00BFFF") // cyan
	ColorMuted   = lipgloss.Color("#666666") // gray
	ColorLabel   = lipgloss.Color("#AAAAAA") // light gray for labels
)

// Box styles
var (
	BoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorInfo).
			Padding(0, 1)

	SafeBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorSafe).
			Padding(0, 1)

	WarningBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorWarning).
			Padding(0, 1)

	DangerBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorDanger).
			Padding(0, 1)
)

// Text styles
var (
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorInfo)

	LabelStyle = lipgloss.NewStyle().
			Foreground(ColorLabel).
			Width(18)

	ValueStyle = lipgloss.NewStyle()

	SafeText = lipgloss.NewStyle().
			Foreground(ColorSafe).
			Bold(true)

	WarningText = lipgloss.NewStyle().
			Foreground(ColorWarning).
			Bold(true)

	DangerText = lipgloss.NewStyle().
			Foreground(ColorDanger).
			Bold(true)

	MutedText = lipgloss.NewStyle().
			Foreground(ColorMuted)

	CodeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#E0E0E0"))
)

// Indicators
const (
	IconSafe    = "✅"
	IconWarning = "⚠"
	IconDanger  = "❌"
	IconInfo    = "ℹ"
)
