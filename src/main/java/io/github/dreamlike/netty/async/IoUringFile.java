package io.github.dreamlike.netty.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.File;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;

public class IoUringFile {

    private final int fd;

    private final IoUringFileIoHandle ioUringIoHandle;

    private final IoEventLoop ioEventLoop;

    private final IoRegistration ioRegistration;

    private IoUringFile(int fd, IoUringFileIoHandle ioUringIoHandle, IoEventLoop ioEventLoop, IoRegistration ioRegistration) {
        this.fd = fd;
        this.ioUringIoHandle = ioUringIoHandle;
        this.ioEventLoop = ioEventLoop;
        this.ioRegistration = ioRegistration;
    }

    public static CompletableFuture<IoUringFile> open(File file, IoEventLoop ioEventLoop, OpenOption... options) {
        return open(file, ioEventLoop, calFlag(options));
    }

    public static CompletableFuture<IoUringFile> open(File file, IoEventLoop ioEventLoop, int openFlag) {

        if (file.isDirectory()) {
            throw new IllegalArgumentException("file is directory");
        }

        if (!file.exists()) {
            throw new IllegalArgumentException("file is not exists");
        }

        if (!ioEventLoop.isCompatible(IoUringIoHandle.class)) {
            throw new IllegalArgumentException("ioEventLoop is not compatible with IoUringIoHandle");
        }

        String absolutePath = file.getAbsolutePath();
        CompletableFuture<IoUringFile> initFuture = new CompletableFuture<>();
        IoUringFileIoHandle ioUringFileIoHandle = new IoUringFileIoHandle(ioEventLoop);

        ioEventLoop.register(ioUringFileIoHandle)
                .addListener(new GenericFutureListener<Future<? super IoRegistration>>() {
                    @Override
                    public void operationComplete(Future<? super IoRegistration> future) throws Exception {
                        if (!future.isSuccess()) {
                            initFuture.completeExceptionally(future.cause());
                            return;
                        }

                        IoRegistration ioUringIoRegistration = (IoRegistration) future.getNow();
                        ByteBuf path = Unpooled.directBuffer(absolutePath.length() + 1);
                        path.writeBytes(absolutePath.getBytes());
                        path.writeByte('\0');
                        ioUringFileIoHandle.openAsync(ioUringIoRegistration, path, openFlag, 0)
                                .whenComplete((syscallResult, t) -> {
                                    path.release();
                                    if (t != null) {
                                        initFuture.completeExceptionally(t);
                                        return;
                                    }
                                    if (syscallResult < 0) {
                                        initFuture.completeExceptionally(Errors.newIOException("IoUringFile::open", syscallResult));
                                        return;
                                    }
                                    initFuture.complete(new IoUringFile(syscallResult, ioUringFileIoHandle, ioEventLoop, ioUringIoRegistration));
                                });
                    }
                });
        return initFuture;
    }

    private static int calFlag(OpenOption... options) {
        int oflags = 0;
        boolean read = false;
        boolean write = false;
        boolean append = false;
        boolean truncateExisting = false;
        boolean noFollowLinks = false;
        boolean create = false;
        boolean createNew = false;
        boolean deleteOnClose = false;
        boolean sync = false;
        boolean dsync = false;
        for (OpenOption option : options) {
            if (option instanceof StandardOpenOption) {
                switch ((StandardOpenOption) option) {
                    case READ:
                        read = true;
                        break;
                    case WRITE:
                        write = true;
                        break;
                    case APPEND:
                        append = true;
                        break;
                    case TRUNCATE_EXISTING:
                        truncateExisting = true;
                        break;
                    case CREATE:
                        create = true;
                        break;
                    case CREATE_NEW:
                        createNew = true;
                        break;
                    case DELETE_ON_CLOSE:
                        deleteOnClose = true;
                        break;
                    case SPARSE: /* ignore */
                        break;
                    case SYNC:
                        sync = true;
                        break;
                    case DSYNC:
                        dsync = true;
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                continue;
            }
            if (option == LinkOption.NOFOLLOW_LINKS) {
                noFollowLinks = true;
                continue;
            }
            if (option == null)
                throw new NullPointerException();
            throw new UnsupportedOperationException(option + " not supported");
        }

        if (read && write) {
            oflags = Constant.O_RDWR;
        } else {
            oflags = (write) ? Constant.O_WRONLY : Constant.O_RDONLY;
        }

        if (write) {
            if (truncateExisting)
                oflags |= Constant.O_TURNC;
            if (append)
                oflags |= Constant.O_APPEND;

            // create flags
            if (createNew) {
                oflags |= (Constant.O_CREAT | Constant.O_EXCL);
            } else {
                if (create)
                    oflags |= Constant.O_CREAT;
            }
        }

        if (dsync)
            oflags |= Constant.O_DSYNC;
        if (sync)
            oflags |= Constant.O_SYNC;

        return oflags;
    }

    public CompletableFuture<Integer> writeAsync(ByteBuf byteBuf, long offset) {
        if (!ioRegistration.isValid()) {
            throw new IllegalStateException("ioRegistration is not valid");
        }

        if (!byteBuf.hasMemoryAddress()) {
            throw new IllegalArgumentException("byteBuf is not direct");
        }

        int len = byteBuf.readableBytes();
        if (len == 0) {
            throw new IllegalArgumentException("len is 0");
        }

        return Helper.syscallTransform("writeAsync", ioUringIoHandle.writeAsync(ioRegistration, byteBuf.retain(), offset, fd))
                .whenComplete((syscallResult, t) -> {
                    byteBuf.release();
                    if (t == null) {
                        byteBuf.readerIndex(byteBuf.readerIndex() + syscallResult);
                    }
                });
    }

    public CompletableFuture<Integer> readAsync(ByteBuf byteBuf, long offset) {
        if (!ioRegistration.isValid()) {
            throw new IllegalStateException("ioRegistration is not valid");
        }
        if (!byteBuf.hasMemoryAddress()) {
            throw new IllegalArgumentException("byteBuf is not direct");
        }
        int len = byteBuf.writableBytes();
        if (len == 0) {
            throw new IllegalArgumentException("len is 0");
        }

        return Helper.syscallTransform("readAsync", ioUringIoHandle.readAsync(ioRegistration, byteBuf.retain(), offset, fd))
                .whenComplete((syscallResult, t) -> {
                    byteBuf.release();
                    if (t == null) {
                        byteBuf.writerIndex(byteBuf.writerIndex() + syscallResult);
                    }
                });
    }

    private static class IoUringFileIoHandle implements IoUringIoHandle {

        private final IoEventLoop ioEventLoop;
        private IntObjectHashMap<CompletableFuture<Integer>> readFuture;
        private short readId = Short.MIN_VALUE;
        private IntObjectHashMap<CompletableFuture<Integer>> writeFuture;
        private short writeId = Short.MIN_VALUE;

        private IoUringFileIoHandle(IoEventLoop ioEventLoop) {
            this.ioEventLoop = ioEventLoop;
            this.readFuture = new IntObjectHashMap<>();
            this.writeFuture = new IntObjectHashMap<>();
        }

        private CompletableFuture<Integer> openAsync(IoRegistration registration, ByteBuf pathCStr, int flags, int mode) {
            IoUringIoOps ioOps = new IoUringIoOps(
                    Constant.IORING_OP_OPENAT, (byte) 0, (short) 0, -1,
                    0L, pathCStr.memoryAddress(), mode, flags,
                    readId, (short) 0, (short) 0, 0, 0L
            );
            CompletableFuture<Integer> openFuture = new CompletableFuture<>();
            readFuture.put(readId, openFuture);
            registration.submit(ioOps);
            return openFuture;
        }

        private CompletableFuture<Integer> writeAsync(IoRegistration registration, ByteBuf buffer, long offset, int fd) {

            CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitWrite(registration, buffer, offset, fd, writeFuture);
            } else {
                ioEventLoop.execute(() -> {
                    submitWrite(registration, buffer, offset, fd, writeFuture);
                });
            }
            return writeFuture;
        }

        private void submitWrite(IoRegistration ioRegistration, ByteBuf buffer, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            while (true) {
                short writeId = this.writeId;
                this.writeId = (short) (writeId + 1);
                if (writeFuture.containsKey(writeId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                        Constant.IORING_OP_WRITE, (byte) 0, (short) 0, fd,
                        offset, buffer.memoryAddress(), buffer.readableBytes(), 0,
                        writeId, (short) 0, (short) 0, 0, 0L
                );
                writeFuture.put(writeId, promise);
                break;
            }
            ioRegistration.submit(ioOps);
        }

        private CompletableFuture<Integer> readAsync(IoRegistration registration, ByteBuf buffer, long offset, int fd) {

            CompletableFuture<Integer> readFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitRead(registration, buffer, offset, fd, readFuture);
            } else {
                ioEventLoop.execute(() -> {
                    submitRead(registration, buffer, offset, fd, readFuture);
                });
            }
            return readFuture;
        }

        private void submitRead(IoRegistration ioRegistration, ByteBuf buffer, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            while (true) {
                short readId = this.readId;
                this.readId = (short) (readId + 1);
                if (readFuture.containsKey(readId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                        Constant.IORING_OP_READ, (byte) 0, (short) 0, fd,
                        offset, buffer.memoryAddress(), buffer.writableBytes(), 0,
                        readId, (short) 0, (short) 0, 0, 0L
                );
                readFuture.put(readId, promise);
                break;
            }
            ioRegistration.submit(ioOps);
        }

        @Override
        public void handle(IoRegistration ioRegistration, IoEvent ioEvent) {
            IoUringIoEvent event = (IoUringIoEvent) ioEvent;
            byte opCode = event.opcode();
            if (opCode == Constant.IORING_OP_OPENAT || opCode == Constant.IORING_OP_READ) {
                CompletableFuture<Integer> future = readFuture.remove(event.data());
                if (future != null) {
                    future.complete(event.res());
                }
                return;
            }

            if (opCode == Constant.IORING_OP_WRITE) {
                CompletableFuture<Integer> future = writeFuture.remove(event.data());
                if (future != null) {
                    future.complete(event.res());
                }
                return;
            }
        }

        @Override
        public void close() throws Exception {

        }
    }
}
