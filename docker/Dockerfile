# =====================================================
# Stage 1: Dependency build (Google-hosted base)
# =====================================================
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base AS builder

WORKDIR /build

COPY requirements.txt .

# Install dependencies into /root/.local (same as before)
RUN pip install --no-cache-dir --upgrade pip==23.3.2 && \
    pip install --user --no-cache-dir -r requirements.txt


# =====================================================
# Stage 2: Runtime image (Flex Template compatible)
# =====================================================
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# 1️⃣ Java (required for BigQuery Storage Write API)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2️⃣ Copy pre-installed Python deps
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/root/.local/lib/python3.10/site-packages

# 3️⃣ Copy project code
COPY . .

# 4️⃣ Install your package (editable-safe)
RUN pip install --no-cache-dir .

# 5️⃣ Flex Template configuration
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
