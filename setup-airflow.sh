#!/bin/bash

echo "🚀 Configurando Airflow Data Lake Project..."

# Criar diretórios necessários
echo "📁 Criando diretórios..."
mkdir -p dags logs plugins config

# Definir permissões corretas
echo "🔐 Configurando permissões..."
if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "darwin"* ]]; then
    sudo chown -R 50000:0 dags/ logs/ plugins/ config/ 2>/dev/null || echo "Aviso: Não foi possível alterar permissões. Execute como root se necessário."
fi

# Verificar se Docker está rodando
echo "🐳 Verificando Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker não está rodando. Inicie o Docker e tente novamente."
    exit 1
fi

# Verificar se docker-compose está disponível
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose não encontrado. Instale o Docker Compose."
    exit 1
fi

# Inicializar Airflow
echo "⚙️ Inicializando Airflow..."
make init

if [ $? -eq 0 ]; then
    echo "✅ Airflow inicializado com sucesso!"
    
    # Iniciar serviços
    echo "🚀 Iniciando serviços..."
    make up
    
    echo ""
    echo "🎉 Setup concluído!"
    echo ""
    echo "📋 Acessos disponíveis:"
    echo "   • Airflow Web UI & API (v3): http://localhost:8085 (airflow/airflow)"
    echo "   • Neo4j Console: http://localhost:7474 (neo4j/password)"
    echo "   • MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    echo "   • Superset: http://localhost:8088 (admin/admin)"
    echo "   • StarRocks FE: http://localhost:8030"
    echo ""
    echo "📝 Comandos úteis:"
    echo "   • make logs     # Ver logs"
    echo "   • make down     # Parar serviços"
    echo "   • make restart  # Reiniciar"
    echo ""
    echo "⏳ Aguarde alguns minutos para todos os serviços ficarem prontos..."
    
else
    echo "❌ Erro na inicialização do Airflow. Verifique os logs:"
    echo "   make logs"
    exit 1
fi