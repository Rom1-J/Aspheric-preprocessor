package prog

import (
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func New(numTrackers int) progress.Writer {
	pw := progress.NewWriter()

	width, _, err := term.GetSize(0)
	if err != nil {
		width = 100
	}

	width -= 50
	pw.SetTrackerLength(width / 4)
	pw.SetMessageWidth(width * 3 / 4)

	pw.SetAutoStop(false)
	pw.SetNumTrackersExpected(numTrackers)
	pw.SetStyle(progress.StyleDefault)
	pw.SetSortBy(progress.SortByMessageDsc)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(time.Millisecond * 100)

	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Colors.Message = text.Colors{text.FgBlue}

	pw.Style().Options.PercentFormat = "%4.1f%%"
	pw.Style().Options.TimeInProgressPrecision = time.Second

	pw.Style().Visibility.ETA = true
	pw.Style().Visibility.ETAOverall = true
	pw.Style().Visibility.Percentage = true
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.SpeedOverall = true
	pw.Style().Visibility.Value = true
	pw.Style().Visibility.Pinned = true

	return pw
}
