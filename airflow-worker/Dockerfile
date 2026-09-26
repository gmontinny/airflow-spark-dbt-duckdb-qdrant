FROM apache/airflow:3.3.1-python3.12

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Instala torch com suporte CUDA 12.4 (RTX 3060 e similares).
# Em máquinas sem GPU o torch CPU já foi instalado pelo requirements.txt
# e este comando apenas atualiza para a versão CUDA — se o registry
# não estiver acessível ou a versão não existir, o build não falha
# pois o torch CPU já está presente e o sentence-transformers usa
# torch.cuda.is_available() para decidir o device em runtime.
RUN pip install --no-cache-dir \
      torch==2.5.1 \
      --index-url https://download.pytorch.org/whl/cu124 \
    || echo "CUDA torch não disponível, mantendo versão CPU"
