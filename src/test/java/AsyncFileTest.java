import io.github.dreamlike.netty.async.op.IoUringFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import org.junit.jupiter.api.AfterAll;
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
    private static MultiThreadIoEventLoopGroup eventLoopGroup;

    @BeforeAll
    public static void init() {
        eventLoopGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        ioEventLoop = eventLoopGroup.next();
    }

    @AfterAll
    public static void destroy() throws IOException {
        eventLoopGroup.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.MILLISECONDS);
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

        Assertions.assertEquals(0, buffer.readableBytes());
    }

    @Test
    public void testRead() throws IOException, ExecutionException, InterruptedException {
        File file = File.createTempFile("sss", ".txt");
        file.deleteOnExit();
        CompletableFuture<IoUringFile> ioUringFileCompletableFuture = IoUringFile.open(file, ioEventLoop, StandardOpenOption.READ, StandardOpenOption.WRITE);
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> ioUringFileCompletableFuture.get());
        byte[] bytes = "hello world".getBytes();
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
        ByteBuf buffer = Unpooled.directBuffer(bytes.length);
        Integer res = ioUringFile.readAsync(buffer, 0).get();
        Assertions.assertEquals(bytes.length, res);
        Assertions.assertArrayEquals(bytes, ByteBufUtil.getBytes(buffer));
    }

    @Test
    public void testReadv() throws IOException, ExecutionException, InterruptedException {
        File file = File.createTempFile("testReadV", ".txt");
        file.deleteOnExit();
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> IoUringFile.open(file, ioEventLoop, StandardOpenOption.READ, StandardOpenOption.WRITE).get());

        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);

        ByteBuf buffer1 = Unpooled.directBuffer(5);
        ByteBuf buffer2 = Unpooled.directBuffer(10);

        Integer res = ioUringFile.readvAsync(0, buffer1, buffer2).get();
        Assertions.assertEquals(bytes.length, res);
        Assertions.assertEquals(5, buffer1.readableBytes());
        Assertions.assertEquals(5, buffer2.readableBytes());
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, ByteBufUtil.getBytes(buffer1));
        Assertions.assertArrayEquals(new byte[]{5, 6, 7, 8, 9}, ByteBufUtil.getBytes(buffer2));
    }

    @Test
    public void testWritev() throws IOException, ExecutionException, InterruptedException {
        File file = File.createTempFile("testWriteV", ".txt");
        file.deleteOnExit();
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> IoUringFile.open(file, ioEventLoop, StandardOpenOption.READ, StandardOpenOption.WRITE).get());
        ByteBuf buffer1 = Unpooled.directBuffer(5);
        ByteBuf buffer2 = Unpooled.directBuffer(10);
        buffer1.writeBytes(new byte[]{0, 1, 2, 3, 4});
        buffer2.writeBytes(new byte[]{5, 6, 7, 8, 9});

        Integer res = ioUringFile.writevAsync(0, buffer1, buffer2).get();
        Assertions.assertEquals(10, res);
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, Files.readAllBytes(file.toPath()));
    }

    @Test
    public void testFsync() throws IOException, ExecutionException, InterruptedException {
        File file = new File("testFsync.txt");
        file.createNewFile();
        file.deleteOnExit();
        IoUringFile ioUringFile = Assertions.assertDoesNotThrow(() -> IoUringFile.open(file, ioEventLoop, StandardOpenOption.READ, StandardOpenOption.WRITE).get());
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
        Assertions.assertEquals(0, ioUringFile.fsync().get());

        Assertions.assertEquals(0, ioUringFile.fdatasync(0, 0).get());
    }
}
