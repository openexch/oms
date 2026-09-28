// SPDX-License-Identifier: Apache-2.0
// Test harness only: exposes a private copy of an existing Java ME recording.
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import java.io.File;

public class ArchiveFixture {
    public static void main(String[] args) throws Exception {
        try (var driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(args[0]).dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED).sharedIdleStrategy(new SleepingMillisIdleStrategy(1)));
             var archive = Archive.launch(new Archive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName()).archiveDir(new File(args[1]))
                .archiveId(77).controlChannel("aeron:udp?endpoint=127.0.0.1:" + args[2])
                .controlStreamId(4010).localControlStreamId(4011)
                .replicationChannel("aeron:udp?endpoint=127.0.0.1:0")
                .recordingEventsEnabled(false).threadingMode(ArchiveThreadingMode.SHARED)
                .idleStrategySupplier(() -> new SleepingMillisIdleStrategy(1)).fileSyncLevel(1).catalogFileSyncLevel(1))) {
            System.out.println("READY"); System.out.flush();
            System.in.read();
        }
    }
}
