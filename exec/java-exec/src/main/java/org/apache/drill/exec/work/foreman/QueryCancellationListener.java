package org.apache.drill.exec.work.foreman;

@FunctionalInterface
public interface QueryCancellationListener {
    void cancel();
}
