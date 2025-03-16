package io.github.dreamlike.netty.async.helper;

import io.netty.channel.unix.Errors;

import java.util.concurrent.CompletableFuture;

public class Helper {
    public static CompletableFuture<Integer> syscallTransform(String method, CompletableFuture<Integer> future) {
        return future.thenCompose(syscall -> {
            if (syscall < 0) {
                return failureFuture(Errors.newIOException(method, syscall));
            } else {
                return CompletableFuture.completedFuture(syscall);
            }
        });
    }

    //java8没有默认的failureFuture
    public static <T> CompletableFuture<T> failureFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
