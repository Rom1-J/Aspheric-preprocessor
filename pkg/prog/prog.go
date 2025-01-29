package prog

import (
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Progress = progress.NewWriter()
var GlobalProgress = ProgressOptsStruct{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func initProgress() {
	width, _, err := term.GetSize(0)
	if err != nil {
		width = 100
	}

	width -= 50
	Progress.SetTrackerLength(width / 4)
	Progress.SetMessageWidth(width * 3 / 4)

	Progress.SetAutoStop(false)
	Progress.SetStyle(progress.StyleDefault)
	Progress.SetSortBy(progress.SortByMessageDsc)
	Progress.SetTrackerPosition(progress.PositionRight)
	Progress.SetUpdateFrequency(time.Millisecond * 100)

	Progress.Style().Colors = progress.StyleColorsExample
	Progress.Style().Colors.Message = text.Colors{text.FgBlue}

	Progress.Style().Options.PercentFormat = "%4.1f%%"
	Progress.Style().Options.TimeInProgressPrecision = time.Second

	Progress.Style().Visibility.ETA = true
	Progress.Style().Visibility.ETAOverall = true
	Progress.Style().Visibility.Percentage = true
	Progress.Style().Visibility.Speed = true
	Progress.Style().Visibility.SpeedOverall = true
	Progress.Style().Visibility.Value = true
	Progress.Style().Visibility.Pinned = true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func New(message string, numTrackers int) ProgressOptsStruct {
	initProgress()

	globalTracker := progress.Tracker{
		Message: message,
		Total:   int64(numTrackers),
		Units:   progress.UnitsDefault,
	}

	Progress.SetNumTrackersExpected(numTrackers)
	Progress.AppendTracker(&globalTracker)

	if logger.ShowProgressbar {
		go Progress.Render()
	}

	GlobalProgress = ProgressOptsStruct{
		Pw:            Progress,
		GlobalTracker: &globalTracker,
	}

	return GlobalProgress
}
