package io.github.dreamlike.netty.async;

class Constant {

    public final static int O_RDONLY = 0;
    public final static int O_WRONLY = 1;
    public final static int O_RDWR = 2;
    public final static int O_CREAT = 64;
    public final static int O_EXCL = 128;
    public final static int O_TURNC = 512;
    public final static int O_APPEND = 1024;
    public final static int O_DIRECT = 16384;
    public final static int O_DSYNC = 4096;
    public final static int O_SYNC = 1052672;
    static final byte IORING_OP_ASYNC_CANCEL = 14;
    static final byte IORING_OP_OPENAT = 18;
    static final byte IORING_OP_CLOSE = 19;
    static final byte IORING_OP_READ = 22;
    static final byte IORING_OP_WRITE = 23;
}
