FROM gcr.io/dataflow-templates-base/python310-template-launcher-base

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# --------------------------------------------------
# 0️⃣ Install Java (REQUIRED for BigQuery Storage Write API)
# --------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# 1️⃣ Install Python dependencies (cache-friendly)
# --------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip==23.3.2 \
    && pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------
# 2️⃣ Copy project code
# --------------------------------------------------
COPY . .

# --------------------------------------------------
# 3️⃣ Install pipeline package (non-editable)
# --------------------------------------------------
RUN pip install --no-cache-dir .

# --------------------------------------------------
# 4️⃣ Flex Template configuration
# --------------------------------------------------
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
