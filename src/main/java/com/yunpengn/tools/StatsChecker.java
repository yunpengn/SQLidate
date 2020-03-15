package com.yunpengn.tools;

import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatsChecker {
  private static final String STATS_PREFIX = "stats";
  private static final Pattern ON_TOP_LINE_FORMAT = Pattern.compile(
      "Out of (\\d+) queries, there are (\\d+) with compensation operators, and (\\d+) with compensation operators on top.");
  private static final Pattern FAILURE_LINE_FORMAT = Pattern.compile(
      "Out of (\\d+) queries, there are (\\d+) failures.");

  public void run(final String folder) throws Exception {
    // Gets all files.
    final File[] files = new File(folder).listFiles();
    assert files != null;

    // Initializes all counters.
    int onTopTotal = 0;
    int onTopCompensation = 0;
    int onTopOnTop = 0;
    int failureTotal = 0;
    int failureFail = 0;

    // Checks each file.
    for (final File file: files) {
      if (!file.isFile() || !file.getName().contains(STATS_PREFIX)) {
        continue;
      }

      // Extracts the content.
      final String[] lines = getLastLines(file, 2);
      final Matcher failureMatcher = FAILURE_LINE_FORMAT.matcher(lines[0]);
      final Matcher onTopMatcher = ON_TOP_LINE_FORMAT.matcher(lines[1]);

      // Updates the counters.
      onTopTotal += Integer.parseInt(onTopMatcher.group(0));
      onTopCompensation += Integer.parseInt(onTopMatcher.group(1));
      onTopOnTop += Integer.parseInt(onTopMatcher.group(2));
      failureTotal += Integer.parseInt(failureMatcher.group(0));
      failureFail += Integer.parseInt(failureMatcher.group(1));
    }

    // Prints out the result.
    System.out.printf("Out of %d queries, there are %d with compensation operators, "
        + "and %d with compensation operators on top.\n", onTopOnTop, onTopCompensation, onTopOnTop);
    System.out.printf("Out of %d queries, there are %d failures.\n", failureTotal, failureFail);
  }

  private String[] getLastLines(final File file, final int n) throws Exception {
    final ReversedLinesFileReader reader = new ReversedLinesFileReader(file, StandardCharsets.UTF_8);

    final String[] result = new String[n];
    for (int i = 0; i < n; i++) {
      final String line = reader.readLine();
      result[i] = line;
    }
    return result;
  }
}
