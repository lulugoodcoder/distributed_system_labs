FROM golang:1.22 AS builder

WORKDIR /app
COPY . .

# 设置 GOPATH 模式
ENV GO111MODULE=off
ENV GOPATH=/go

# 创建正确的目录结构来匹配脚本中的路径
RUN mkdir -p /go/src/6.5840 && \
    ln -sf /app/src/mr /go/src/6.5840/mr && \
    ln -sf /app/src/main /go/src/6.5840/main && \
    ln -sf /app/src/mrapps /go/src/6.5840/mrapps

# 在正确的位置构建所有内容（模拟脚本的构建过程）
RUN cd /go/src/6.5840/mrapps && \
    for app in wc.go indexer.go mtiming.go rtiming.go jobcount.go early_exit.go crash.go nocrash.go; do \
        if [ -f "$app" ]; then \
            plugin_name=$(basename "$app" .go); \
            echo "Building plugin: $plugin_name.so"; \
            go build -buildmode=plugin -o "/app/$plugin_name.so" "$app"; \
        fi; \
    done

RUN cd /go/src/6.5840/main && \
    go build -o /app/mrcoordinator mrcoordinator.go && \
    go build -o /app/mrworker mrworker.go && \
    go build -o /app/mrsequential mrsequential.go

# 运行时镜像
FROM debian:bookworm-slim

# 安装必要的依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    bash \
    coreutils \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 复制所有构建好的文件
COPY --from=builder /app/mrcoordinator /app/mrworker /app/mrsequential /app/
COPY --from=builder /app/*.so /app/


COPY src/main/pg-*.txt /app/

# 创建测试脚本期望的目录结构
RUN mkdir -p /go/src/6.5840 && \
    ln -sf /app /go/src/6.5840/main && \
    ln -sf /app /go/src/6.5840/mrapps

# 设置默认命令
CMD ["bash"]

