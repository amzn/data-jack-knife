package com.amazon.djk.report;

public class ProgressReporter extends Thread {
	private final long sleepMillis;
	private boolean done = false;
	private final ProgressReport report;
	private final DisplayMode mode;
	private final GraphDisplay graphDisplay = new GraphDisplay();

	/**
	 * constructor when Reporter run as thread
	 * 
	 * @param report
	 * @param reportIntervalSecs
	 */
	public ProgressReporter(ProgressReport report, int reportIntervalSecs, DisplayMode mode) {
		this.report = report;
		this.mode = mode;
		sleepMillis = reportIntervalSecs * 1000;
	}
	
	/**
	 * constructor when main thread calls report();
	 * 
	 * @param report
	 */
	public ProgressReporter(ProgressReport report) {
		this.report = report;
		this.mode = DisplayMode.SHOW;
		sleepMillis = 0; // unused when another thread calls report();
	}
	
	
	public void update() {
        report.update();
        if (mode != DisplayMode.NO_SHOW) {
            graphDisplay.clear();
            report.display(graphDisplay);
            graphDisplay.render();
        }
	}
	
	
	@Override
	public void run() {
		
		switch (mode) {
		case NO_SHOW_IF_FAST:
			noDisplayIfFastRun();
			return;

		case NO_SHOW:			
		case SHOW:
		case SHOW_ONCE:
		case FORCE_SHOW:
			otherRun();
			return;
		}
	}
	
	/**
	 * 
	 */
	private void noDisplayIfFastRun() {
		boolean firstTime = true;
		while (!done) {
			try {
				Thread.sleep(sleepMillis); 
			}
			
			catch (InterruptedException e) {
				// means we won't ever have displayed
				if (firstTime) return;
				done = true;
			}
			
			update();
			firstTime = false;
		}
	}
	
	/**
	 * 
	 */
	private void otherRun() {
		while (!done) {
			if (mode != DisplayMode.SHOW_ONCE) {
			    update();
			}
			
			try {
				Thread.sleep(sleepMillis); 
			}
			
			catch (InterruptedException e) {
				done = true;
			}
		}
		
		// final display
		update();
	}

	/**
	 * 
	 */
	public void stopThread() {
		done = true;
	}
}
