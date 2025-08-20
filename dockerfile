
FROM python:3.13.5

# 构建依赖
WORKDIR /app

COPY requirements.txt .

RUN pip install  -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

RUN pip install --upgrade pip

RUN pip install  -r requirements.txt

# 3. 复制项目代码
COPY . .

# 4. 切换到 agent 环境
ENTRYPOINT ["sh", "-c"]
ENV PYTHONPATH=/app

# 5. 暴露端口
EXPOSE 8060

# 6. 启动应用

CMD ["python src/app.py"]