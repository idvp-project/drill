package org.apache.drill.exec.physical.impl.spill;

import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.test.OperatorFixture;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Category(UnlikelyTest.class)
public class SpillSetTest extends ExecTest {
    private final DrillConfig drillConf = DrillConfig.create();

    @Test
    public void testDrillBufSpill() throws IOException {
        testSpillSet("none");
    }

    @Test
    public void testDrillBufSpillGzip() throws IOException {
        testSpillSet("gzip");
    }

    @Test
    public void testDrillBufSpillSnappy() throws IOException {
        testSpillSet("snappy");
    }

    private void testSpillSet(String compression) throws IOException {
        OperatorFixture fixture = createOperatorFixture(compression);
        SpillSet spillSet = new SpillSet(fixture.getFragmentContext(), new HashAggregate(null, null, null, null, 0));
        String spillFile = spillSet.getNextSpillFile();
        try {
            byte[] value = RandomStringUtils.randomAlphanumeric(200_000).getBytes(StandardCharsets.UTF_8);

            DrillBuf buf = fixture.getFragmentContext().getAllocator().buffer(256 * 1024);
            int bytes;
            try {
                buf.writeBytes(value);
                try (OutputStream stream = Channels.newOutputStream(spillSet.openForOutput(spillFile))) {
                    bytes = buf.readableBytes();
                    fixture.getFragmentContext().getAllocator().write(buf, bytes, stream);
                }
            } finally {
                buf.release();
            }

            buf = fixture.getFragmentContext().getAllocator().buffer(256 * 1024);
            try (InputStream is = spillSet.openForInput(spillFile)) {
                fixture.getFragmentContext().getAllocator().read(buf, bytes, is);
                byte[] content = new byte[value.length];
                buf.readBytes(content, 0, content.length);

                Assert.assertArrayEquals(value, content);
            } finally {
                buf.release();
            }

            try {
                long spillSize = Files.size(Paths.get(spillFile));
                double ratio = spillSize / (value.length * 1.0);
                System.out.printf("Compress ratio of '%s': %s\n", compression, ratio);
            } catch (Exception e) {
                // ignore
            }
        } finally {
            spillSet.delete(spillFile);
        }
    }

    private OperatorFixture createOperatorFixture(String compression) {
        final OperatorFixture.Builder builder = new OperatorFixture.Builder(dirTestWatcher);
        builder.configBuilder().configProps(drillConf)
                .put(ExecConstants.HASHAGG_SPILL_COMPRESSION, compression)
                .put(ExecConstants.HASHJOIN_SPILL_COMPRESSION, compression)
                .put(ExecConstants.EXTERNAL_SORT_SPILL_COMPRESSION, compression)
                .put(ExecConstants.SPILL_COMPRESSION, compression)
                .put(ExecConstants.IMPERSONATION_ENABLED, false);
        return builder.build();
    }

}
