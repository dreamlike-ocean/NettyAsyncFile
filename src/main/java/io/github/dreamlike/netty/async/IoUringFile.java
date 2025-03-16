package io.github.dreamlike.netty.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.IovArray;
import io.netty.channel.unix.Limits;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoUringFile implements AutoCloseable {

    private final int fd;

    private final IoUringFileIoHandle ioUringIoHandle;

    private final IoEventLoop ioEventLoop;

    private final IoRegistration ioRegistration;

    private IoUringFile(int fd, IoUringFileIoHandle ioUringIoHandle, IoEventLoop ioEventLoop, IoRegistration ioRegistration) {
        this.fd = fd;
        this.ioUringIoHandle = ioUringIoHandle;
        this.ioEventLoop = ioEventLoop;
        this.ioRegistration = ioRegistration;
        ioUringIoHandle.ioUringFile = this;
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
                        ioUringFileIoHandle.registration = ioUringIoRegistration;
                        ioUringFileIoHandle.openAsync(path, openFlag, 0)
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
        allowSubmit();

        if (!byteBuf.hasMemoryAddress()) {
            throw new IllegalArgumentException("byteBuf is not direct");
        }

        int len = byteBuf.readableBytes();
        if (len == 0) {
            throw new IllegalArgumentException("len is 0");
        }

        return Helper.syscallTransform("writeAsync", ioUringIoHandle.writeAsync(byteBuf.retain(), offset, fd))
                .whenComplete((syscallResult, t) -> {
                    byteBuf.release();
                    if (t == null) {
                        byteBuf.readerIndex(byteBuf.readerIndex() + syscallResult);
                    }
                });
    }

    public CompletableFuture<Integer> readAsync(ByteBuf byteBuf, long offset) {
        allowSubmit();

        if (!byteBuf.hasMemoryAddress()) {
            throw new IllegalArgumentException("byteBuf is not direct");
        }
        int len = byteBuf.writableBytes();
        if (len == 0) {
            throw new IllegalArgumentException("len is 0");
        }

        return Helper.syscallTransform("readAsync", ioUringIoHandle.readAsync(byteBuf.retain(), offset, fd))
                .whenComplete((syscallResult, t) -> {
                    byteBuf.release();
                    if (t == null) {
                        byteBuf.writerIndex(byteBuf.writerIndex() + syscallResult);
                    }
                });
    }

    public CompletableFuture<Integer> readvAsync(long offset, ByteBuf... readBuffers) {
        allowSubmit();
        IovArray iovArray = createSafeIovArrayForRead(readBuffers);
        return Helper.syscallTransform("readvAsync", ioUringIoHandle.readvAsync(iovArray, offset, fd))
                .whenComplete((syscallResult, t) -> {
                    iovArray.release();
                    if (t == null) {
                        progressReadBuffer(readBuffers, syscallResult);
                    }
                    for (ByteBuf readBuffer : readBuffers) {
                        readBuffer.release();
                    }
                });
    }

    public CompletableFuture<Integer> writevAsync(long offset, ByteBuf... writeBuffers) {
        allowSubmit();
        IovArray iovArray = createSafeIovArrayForWrite(writeBuffers);
        return Helper.syscallTransform("writevAsync", ioUringIoHandle.writevAsync(iovArray, offset, fd))
                .whenComplete((syscallResult, t) -> {
                    iovArray.release();
                    if (t == null) {
                        progressWriteBuffer(writeBuffers, syscallResult);
                    }
                    for (ByteBuf writeBuffer : writeBuffers) {
                        writeBuffer.release();
                    }
                });
    }

    private IovArray createSafeIovArrayForRead(ByteBuf... readBuffers) {
        if (readBuffers.length == 0) {
            throw new IllegalArgumentException("readBuffers is empty");
        }

        if (readBuffers.length > Limits.IOV_MAX) {
            throw new IllegalArgumentException("readBuffers is too many");
        }

        boolean validIov = true;
        IovArray iovArray = new IovArray();
        for (ByteBuf readBuffer : readBuffers) {
            if (!readBuffer.hasMemoryAddress()) {
                validIov = false;
                break;
            }
            iovArray.add(readBuffer, readBuffer.writerIndex(), readBuffer.writableBytes());
        }
        if (!validIov) {
            iovArray.release();
            throw new IllegalArgumentException("readBuffers is not direct");
        }

        //retain
        for (ByteBuf readBuffer : readBuffers) {
            readBuffer.retain();
        }
        return iovArray;
    }

    private IovArray createSafeIovArrayForWrite(ByteBuf... writeBuffers) {
        if (writeBuffers.length == 0) {
            throw new IllegalArgumentException("writeBuffers is empty");
        }

        if (writeBuffers.length > Limits.IOV_MAX) {
            throw new IllegalArgumentException("writeBuffers is too many");
        }

        boolean validIov = true;
        IovArray iovArray = new IovArray();
        for (ByteBuf readBuffer : writeBuffers) {
            if (!readBuffer.hasMemoryAddress()) {
                validIov = false;
                break;
            }
            iovArray.add(readBuffer, readBuffer.readerIndex(), readBuffer.readableBytes());
        }
        if (!validIov) {
            iovArray.release();
            throw new IllegalArgumentException("writeBuffers is not direct");
        }

        //retain
        for (ByteBuf writeBuffer : writeBuffers) {
            writeBuffer.retain();
        }
        return iovArray;
    }

    private void progressReadBuffer(ByteBuf[] byteBufs, int syscallResult) {
        int readLen = syscallResult;
        for (ByteBuf buf : byteBufs) {
            if (readLen == 0) {
                break;
            }
            ByteBuf byteBuf = buf;
            int progress = Math.min(byteBuf.writableBytes(), readLen);
            byteBuf.writerIndex(byteBuf.writerIndex() + progress);
            readLen -= progress;
        }
    }

    private void progressWriteBuffer(ByteBuf[] byteBufs, int syscallResult) {
        int writeLen = syscallResult;
        for (ByteBuf buf : byteBufs) {
            if (writeLen == 0) {
                break;
            }
            ByteBuf byteBuf = buf;
            int progress = Math.min(byteBuf.readableBytes(), writeLen);
            byteBuf.readerIndex(byteBuf.readerIndex() + progress);
            writeLen -= progress;
        }
    }

    private void allowSubmit() {
        boolean needThrow = !ioRegistration.isValid() || isClosed();
        if (needThrow) {
            throw new IllegalStateException("ioRegistration is not valid or file is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (ioUringIoHandle.isClosed.compareAndSet(false, true)) {
            ioRegistration.cancel();
        }
        ioUringIoHandle.cancelAllAsync();
    }

    public boolean isClosed() {
        return ioUringIoHandle.isClosed.get();
    }

    private static class AsyncOpContext {
        private final CompletableFuture<Integer> future;
        private final byte opsCode;
        private long uringId;

        private AsyncOpContext(CompletableFuture<Integer> future, byte opsCode) {
            this.future = future;
            this.opsCode = opsCode;
        }
    }

    private static class IoUringFileIoHandle implements IoUringIoHandle {

        private final AtomicBoolean isClosed;
        private final IoEventLoop ioEventLoop;
        private IntObjectHashMap<AsyncOpContext> readFutures;
        private short readId = Short.MIN_VALUE;
        private IntObjectHashMap<AsyncOpContext> writeFuture;
        private short writeId = Short.MIN_VALUE;
        private IoUringFile ioUringFile;
        private IoRegistration registration;

        private IoUringFileIoHandle(IoEventLoop ioEventLoop) {
            this.ioEventLoop = ioEventLoop;
            this.readFutures = new IntObjectHashMap<>();
            this.writeFuture = new IntObjectHashMap<>();
            this.isClosed = new AtomicBoolean(false);
        }

        static IoUringIoOps newAsyncCancel(byte flags, long userData, short data) {
            return new IoUringIoOps(Constant.IORING_OP_ASYNC_CANCEL, flags, (short) 0, -1, 0, userData, 0, 0,
                    data, (short) 0, (short) 0, 0, 0);
        }

        private CompletableFuture<Integer> openAsync(ByteBuf pathCStr, int flags, int mode) {
            IoUringIoOps ioOps = new IoUringIoOps(
                    Constant.IORING_OP_OPENAT, (byte) 0, (short) 0, -1,
                    0L, pathCStr.memoryAddress(), mode, flags,
                    readId, (short) 0, (short) 0, 0, 0L
            );
            CompletableFuture<Integer> openFuture = new CompletableFuture<>();
            AsyncOpContext context = new AsyncOpContext(openFuture, Constant.IORING_OP_OPENAT);
            readFutures.put(readId, context);
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                openFuture.completeExceptionally(new IOException("submit openat failed"));
            } else {
                context.uringId = uringId;
            }
            return openFuture;
        }

        private CompletableFuture<Integer> writeAsync(ByteBuf buffer, long offset, int fd) {

            CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitWrite(buffer, offset, fd, writeFuture);
            } else {
                ioEventLoop.execute(() -> {
                    submitWrite(buffer, offset, fd, writeFuture);
                });
            }
            return writeFuture;
        }

        private void submitWrite(ByteBuf buffer, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_WRITE);
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
                writeFuture.put(writeId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit write failed"));
            } else {
                context.uringId = uringId;
            }
        }

        private void submitCloseAsync(int fd) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps closeOps = new IoUringIoOps(Constant.IORING_OP_CLOSE, (byte) 0, (short) 0, fd, 0L, 0L, 0, 0, (short) 0, (short) 0, (short) 0, 0, 0L);
            registration.submit(closeOps);
        }

        private void cancelAllAsync() {

            if (!ioEventLoop.inEventLoop()) {
                ioEventLoop.execute(this::cancelAllAsync);
                return;
            }

            assert ioEventLoop.inEventLoop();
            short cancelId = 0;
            IoRegistration ioRegistration = this.registration;
            for (AsyncOpContext context : readFutures.values()) {
                IoUringIoOps ops = newAsyncCancel((byte) 0, context.uringId, cancelId);
                ioRegistration.submit(ops);
                cancelId++;
            }
            for (AsyncOpContext context : writeFuture.values()) {
                IoUringIoOps ops = newAsyncCancel((byte) 0, context.uringId, cancelId);
                ioRegistration.submit(ops);
                cancelId++;
            }
        }

        private CompletableFuture<Integer> readAsync(ByteBuf buffer, long offset, int fd) {

            CompletableFuture<Integer> readFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitRead(buffer, offset, fd, readFuture);
            } else {
                ioEventLoop.execute(() -> submitRead(buffer, offset, fd, readFuture));
            }
            return readFuture;
        }

        private void submitRead(ByteBuf buffer, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_READ);
            while (true) {
                short readId = this.readId;
                this.readId = (short) (readId + 1);
                if (readFutures.containsKey(readId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                        Constant.IORING_OP_READ, (byte) 0, (short) 0, fd,
                        offset, buffer.memoryAddress(), buffer.writableBytes(), 0,
                        readId, (short) 0, (short) 0, 0, 0L
                );
                readFutures.put(readId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit read failed"));
            } else {
                context.uringId = uringId;
            }
        }

        private CompletableFuture<Integer> readvAsync(IovArray iovArray, long offset, int fd) {
            CompletableFuture<Integer> readFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitReadv(iovArray, offset, fd, readFuture);
            } else {
                ioEventLoop.execute(() -> submitReadv(iovArray, offset, fd, readFuture));
            }
            return readFuture;
        }


        private void submitReadv(IovArray iovArray, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_READV);
            while (true) {
                short readId = this.readId;
                this.readId = (short) (readId + 1);
                if (readFutures.containsKey(readId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                        Constant.IORING_OP_READV, (byte) 0, (short) 0, fd,
                        offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                        readId, (short) 0, (short) 0, 0, 0L
                );
                readFutures.put(readId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit read failed"));
            } else {
                context.uringId = uringId;
            }
        }

        private CompletableFuture<Integer> writevAsync(IovArray iovArray, long offset, int fd) {
            CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
            if (ioEventLoop.inEventLoop()) {
                submitWritev(iovArray, offset, fd, writeFuture);
            } else {
                ioEventLoop.execute(() -> submitWritev(iovArray, offset, fd, writeFuture));
            }
            return writeFuture;
        }

        private void submitWritev(IovArray iovArray, long offset, int fd, CompletableFuture<Integer> promise) {
            assert ioEventLoop.inEventLoop();
            IoUringIoOps ioOps = null;
            AsyncOpContext context = new AsyncOpContext(promise, Constant.IORING_OP_WRITEV);
            while (true) {
                short writeId = this.writeId;
                this.writeId = (short) (writeId + 1);
                if (writeFuture.containsKey(writeId)) {
                    continue;
                }
                ioOps = new IoUringIoOps(
                        Constant.IORING_OP_WRITEV, (byte) 0, (short) 0, fd,
                        offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                        writeId, (short) 0, (short) 0, 0, 0L
                );
                writeFuture.put(writeId, context);
                break;
            }
            long uringId = registration.submit(ioOps);
            if (uringId == -1) {
                promise.completeExceptionally(new IOException("submit read failed"));
            } else {
                context.uringId = uringId;
            }
        }

        @Override
        public void handle(IoRegistration ioRegistration, IoEvent ioEvent) {
            IoUringIoEvent event = (IoUringIoEvent) ioEvent;
            byte opCode = event.opcode();
            if (opCode == Constant.IORING_OP_OPENAT || opCode == Constant.IORING_OP_READ || opCode == Constant.IORING_OP_READV) {
                AsyncOpContext asyncOpContext = readFutures.remove(event.data());
                if (asyncOpContext != null) {
                    asyncOpContext.future.complete(event.res());
                }
            }

            if (opCode == Constant.IORING_OP_WRITE || opCode == Constant.IORING_OP_WRITEV) {
                AsyncOpContext asyncOpContext = writeFuture.remove(event.data());
                if (asyncOpContext != null) {
                    asyncOpContext.future.complete(event.res());
                }
            }

            if (opCode == Constant.IORING_OP_CLOSE) {
                return;
            }

            if (isClosed.get() && readFutures.isEmpty() && writeFuture.isEmpty()) {
                submitCloseAsync(ioUringFile.fd);
                return;
            }
        }

        @Override
        public void close() throws Exception {
            ioUringFile.close();
        }
    }
}
