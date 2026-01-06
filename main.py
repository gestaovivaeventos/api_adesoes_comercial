from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware # <<< 1. IMPORTAÇÃO ADICIONADA
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# --- 2. BLOCO DE CÓDIGO CORS ADICIONADO ---
# Este bloco funciona como um "porteiro" que autoriza o seu dashboard a acessar a API.
origins = [
    # O "*" permite que QUALQUER site acesse sua API. Para mais segurança no futuro,
    # você pode substituir por ["https://seu-dashboard.vercel.app", "https://seu-dashboard.streamlit.app"]
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Permite todos os métodos (GET, POST, etc)
    allow_headers=["*"], # Permite todos os cabeçalhos
)
# --- FIM DO BLOCO CORS ---


pool = None
try:
    pool = SimpleConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), cursor_factory=RealDictCursor
    )
except psycopg2.OperationalError as e:
    print(f"ERRO CRÍTICO: Falha ao inicializar o pool de conexões. {e}")


@app.get("/")
def health_check():
    return {"status": "ok"}


@app.get("/dados")
def obter_dados(limit: int = 5000, offset: int = 0): # Adiciona parâmetros de paginação
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # Adicionamos LIMIT e OFFSET à sua query original
            query = """
                SELECT
	CASE
		WHEN u.nm_unidade = 'Campos' THEN 'Itaperuna Muriae'
		ELSE u.nm_unidade
	END AS nm_unidade,
	i.id AS codigo_integrante,
	i.nm_integrante,
	CASE
		WHEN f.is_fundo_assessoria_pura_convertido IS TRUE THEN f.dt_conversao_ass_pura
		WHEN f.is_fundo_assessoria_pura_convertido IS FALSE THEN i.dt_cadastro
	END AS dt_cadastro_integrante,
	f.id AS id_fundo,
	f.nm_fundo AS nm_fundo,
	c.nm_curso AS curso_fundo,
	CASE
		WHEN f.tp_servico = '1' THEN 'Pacote'
		WHEN f.tp_servico = '2' THEN 'Assessoria'
		WHEN f.tp_servico = '3' THEN 'Super Integrada'
	END AS tp_servico,
	CASE
		WHEN (
			f.dt_contrato IS NULL
			OR f.dt_contrato > f.dt_cadastro
		) THEN f.dt_cadastro
		WHEN f.dt_contrato IS NOT NULL THEN f.dt_contrato
	END AS dt_contrato,
	f.dt_cadastro AS dt_cadastro_fundo,
	'' AS total_lancamentos,
	fc.vl_plano AS vl_plano,
	'' AS cadastrado_por,
	CASE
		WHEN us.cpf IS NULL THEN us.nome
		ELSE NULL
	END AS indicado_por,
	CASE
		WHEN us.fl_consultor_comercial IS TRUE THEN 'Sim'
		WHEN us.fl_consultor_comercial IS FALSE THEN 'Não'
	END AS consultor_comercial,
	it.nm_instituicao,
	i.fl_ativo AS fl_ativo,
	CASE
		WHEN f.tipocliente_id = 15 THEN 'Fundo de formatura'
		WHEN f.tipocliente_id = 17 THEN 'Pre evento'
	END AS tipo_cliente,
CASE
		WHEN us.fl_consultor_comercial IS NOT TRUE AND (
			( -- Data 2 (dt_cadastro_integrante)
				CASE
					WHEN f.is_fundo_assessoria_pura_convertido IS TRUE THEN f.dt_conversao_ass_pura
					ELSE i.dt_cadastro
				END
			)::date
			- 
			( -- Data 1 (dt_contrato)
				CASE
					WHEN (f.dt_contrato IS NULL OR f.dt_contrato > f.dt_cadastro) THEN f.dt_cadastro
					WHEN f.dt_contrato IS NOT NULL THEN f.dt_contrato
				END
			)::date 
		) > 60 THEN 'PÓS VENDA'
		ELSE 'VENDA'
	END AS venda_posvenda
FROM
	tb_fundo f
	JOIN tb_unidade u ON f.unidade_id = u.id
	JOIN tb_integrante i ON i.fundo_id = f.id
	LEFT JOIN tb_fundo_cota fc ON fc.cota_id = i.cota_id
	AND i.fundo_id = fc.fundo_id
	JOIN tb_curso c ON c.id = f.curso_id
	LEFT JOIN tb_usuario us ON us.id = i.id_usuario_indicacao
	LEFT JOIN tb_instituicao it ON f.instituicao_id = it.id
WHERE
	u.categoria = '2'
	AND f.tipocliente_id IN (15, 17)
	AND i.dt_cadastro >= '2019-01-01'
	AND f.is_fundo_teste IS FALSE
	AND i.nu_status NOT IN (11, 9, 8, 13, 14)
	AND f.is_assessoria_pura IS FALSE
	AND (
		i.dt_cadastro <= '2024-03-08'
		OR (
			i.dt_cadastro > '2024-03-08'
			AND i.id NOT IN (
				SELECT
					i2.id
				FROM
					tb_integrante i2
				WHERE
					i2.forma_adesao = 6
					AND i2.dt_cadastro > '2024-03-08'
			)
		)
	)
ORDER BY
	i.dt_cadastro
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset)) # Passa os valores de forma segura
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)
