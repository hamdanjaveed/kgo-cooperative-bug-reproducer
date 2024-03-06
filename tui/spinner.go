package tui

import (
  "github.com/charmbracelet/bubbles/spinner"
  tea "github.com/charmbracelet/bubbletea"
  "github.com/charmbracelet/lipgloss"
)

var helpStyle = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "236", Dark: "248"})

type spinnerModel struct {
  msg       string
  msgUpdate *chan string
  spinner   spinner.Model
  quitting  bool
}

func NewSpinner(msg string, msgUpdate *chan string) tea.Model {
  s := spinner.New()
  s.Spinner = spinner.Points
  s.Style = lipgloss.NewStyle().
    Foreground(lipgloss.Color("205")).
    PaddingRight(2)
  return spinnerModel{
    msg:       msg,
    msgUpdate: msgUpdate,
    spinner:   s,
    quitting:  false,
  }
}

func (m spinnerModel) Init() tea.Cmd {
  return m.spinner.Tick
}

func (m spinnerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
  switch msg := msg.(type) {
  case tea.KeyMsg:
    switch msg.String() {
    case "q", "esc", "ctrl+c":
      m.quitting = true
      return m, tea.Quit
    default:
      return m, nil
    }

  default:
    if m.msgUpdate != nil {
      select {
      case newMsg := <-*m.msgUpdate:
        m.msg = newMsg
      default:
      }
    }

    var cmd tea.Cmd
    m.spinner, cmd = m.spinner.Update(msg)
    return m, cmd
  }
}

func (m spinnerModel) View() string {
  str := lipgloss.JoinHorizontal(lipgloss.Center, m.spinner.View(), m.msg)
  str = lipgloss.JoinVertical(lipgloss.Left, "", str, "", helpStyle.Render("press 'q' to quit"))
  if m.quitting {
    return str + "\n"
  }
  return str
}
