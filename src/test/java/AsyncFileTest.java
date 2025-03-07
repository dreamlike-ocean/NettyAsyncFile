import io.github.dreamlike.netty.async.IoUringFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AsyncFileTest {

    private static IoEventLoop ioEventLoop;

    @BeforeAll
    public static void init() {
        MultiThreadIoEventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        ioEventLoop = eventLoopGroup.next();
    }

    @Test
    public void testOpen() throws IOException {
        File file = File.createTempFile("sss", ".txt");
        CompletableFuture<IoUringFile> ioUringFileCompletableFuture = IoUringFile.open(file, ioEventLoop);
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> ioUringFileCompletableFuture.get());
        Assertions.assertNotNull(ioUringFile);
    }

    @Test
    public void testWrite() throws IOException, ExecutionException, InterruptedException {
        File file = File.createTempFile("sss", ".txt");
        file.deleteOnExit();
        CompletableFuture<IoUringFile> ioUringFileCompletableFuture = IoUringFile.open(file, ioEventLoop, StandardOpenOption.READ, StandardOpenOption.WRITE);
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> ioUringFileCompletableFuture.get());
        byte[] bytes = "hello world".getBytes();
        ByteBuf buffer = Unpooled.directBuffer();
        buffer.writeBytes(bytes);
        Integer res = ioUringFile.writeAsync(buffer, 0).get();
        Assertions.assertEquals(bytes.length, res);
        Assertions.assertArrayEquals(bytes, Files.readAllBytes(file.toPath()));
    }
}
