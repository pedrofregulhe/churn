import pandas as pd
import os
import streamlit as st
import plotly.express as px
from datetime import datetime
import io


# --- 1. Configurações e Caminhos ---
data_dir = '.' # Caminho ajustado para o diretório atual na raiz do repositório GitHub
file_2024 = 'churn_2024.xlsx'
file_2025 = 'churn_2025.xlsx'
output_file_powerbi = 'dados_churn_consolidados_for_powerbi.xlsx'
output_path_powerbi = os.path.join(data_dir, output_file_powerbi)

file_active_base = 'base_ativa_clientes.xlsx'


# --- Função para Carregar e Transformar Dados ---
@st.cache_data
def load_and_transform_data(data_folder, file_2024, file_2025, file_active_base):
    """
    Carrega e combina os dados de churn de diferentes anos e a base ativa,
    aplicando todas as transformações necessárias.
    """
    df_churn = pd.DataFrame()
    df_combined = pd.DataFrame()

    # Carregamento e transformação dos dados de CHURN
    try:
        df_2024 = pd.read_excel(os.path.join(data_folder, file_2024))
        df_2025 = pd.read_excel(os.path.join(data_folder, file_2025))
        df_combined = pd.concat([df_2024, df_2025], ignore_index=True)

    except FileNotFoundError as e:
        st.error(f"ERRO: Arquivo .xlsx de CHURN não encontrado. Detalhes: {e}")
        st.stop()
    except Exception as e:
        st.error(f"ERRO: Problema ao carregar ou combinar dados de CHURN: {e}")
        st.stop()

    # --- INÍCIO: Carregamento e transformação dos dados da BASE ATIVA ---
    df_active_processed = pd.DataFrame()
    try:
        df_active_raw = pd.read_excel(os.path.join(data_dir, file_active_base))

        df_active_raw.rename(columns={
            'Data': 'Data Base Ativa',
            'Tipo Cliente': 'Tipo de Cliente Base Ativa Raw',
            'Volume Clientes Ativos': 'Volume Base Ativa'
        }, inplace=True)

        df_active_raw['Data Base Ativa'] = pd.to_datetime(df_active_raw['Data Base Ativa'], errors='coerce')
        df_active_raw['Volume Base Ativa'] = pd.to_numeric(df_active_raw['Volume Base Ativa'], errors='coerce')

        df_active_raw.dropna(subset=['Data Base Ativa', 'Volume Base Ativa'], inplace=True)
        df_active_raw['Volume Base Ativa'] = df_active_raw['Volume Base Ativa'].astype(int)

        df_active_raw['Ano Base Ativa'] = df_active_raw['Data Base Ativa'].dt.year.astype(int)
        df_active_raw['Mes Base Ativa'] = df_active_raw['Data Base Ativa'].dt.month.astype(int)
        
        month_names_map = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                           7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
        df_active_raw['Nome Mes Ativa'] = df_active_raw['Mes Base Ativa'].map(month_names_map)

        df_active_raw['Tipo de Cliente Base Ativa Raw'] = df_active_raw['Tipo de Cliente Base Ativa Raw'].astype(str).str.strip().str.replace('\xa0', ' ')
        df_active_raw['Tipo de Cliente Base Ativa'] = df_active_raw['Tipo de Cliente Base Ativa Raw'].apply(map_tipo_cliente)

        df_active_processed = df_active_raw.drop(columns=['Tipo de Cliente Base Ativa Raw'])

    except FileNotFoundError as e:
        st.warning(f"AVISO: Arquivo .xlsx da BASE ATIVA não encontrado. A projeção da base ativa não será exibida. Detalhes: {e}")
        df_active_processed = pd.DataFrame()
    except Exception as e:
        print(f"Erro detalhado na BASE ATIVA (Transformação): {e}")
        st.warning(f"AVISO: Problema ao carregar ou transformar dados da BASE ATIVA. A projeção da base ativa pode estar incorreta. Detalhes: {e}")
        df_active_processed = pd.DataFrame()

    # FIM: Carregamento e transformação dos dados da BASE ATIVA ---

    # Continuação da transformação dos dados de CHURN
    df_combined.rename(columns={
        'Datacriacaoos': 'Data de Criacao da OS',
        'Statusos': 'Status da OS',
        'DATADESINSTALACAO': 'Data de Desinstalacao',
        'Formajuridica': 'Forma Juridica Original',
        'tipoChurn': 'Tipo de Churn', # Manter este mapeamento para Tipo de Churn (global)
        'Filialos': 'Filial' # NOVO: Mapear 'Filialos' para 'Filial'
    }, inplace=True)

    # NOVO: Adicionar 'Categoria4' como uma coluna separada se existir
    if 'Categoria4' in df_combined.columns:
        df_combined['Categoria4_Motivo'] = df_combined['Categoria4'].astype(str)
    else:
        df_combined['Categoria4_Motivo'] = None # Ou uma string vazia/NaN se preferir

    # O filtro 'desconsiderar' continua se aplicando à coluna 'Tipo de Churn'
    if 'Tipo de Churn' in df_combined.columns:
        df_combined = df_combined[df_combined['Tipo de Churn'].astype(str).str.strip().str.lower() != 'desconsiderar'].copy()


    df_combined['Data de Criacao da OS'] = pd.to_datetime(df_combined['Data de Criacao da OS'], errors='coerce')
    df_combined['Data de Desinstalacao'] = pd.to_datetime(df_combined['Data de Desinstalacao'], errors='coerce')

    if 'Status da OS' in df_combined.columns:
        df_churn = df_combined[df_combined['Status da OS'].astype(str).str.strip().str.contains('Concluído', na=False, case=False)].copy()
    else:
        st.warning("AVISO: Coluna 'Status da OS' não encontrada para filtrar churn. df_churn pode estar vazio.")
        df_churn = pd.DataFrame()

    if not df_churn.empty:
        # Usando 'Forma Juridica Original' para 'Tipo de Cliente' (PF/PME/Corporativo)
        df_churn['Tipo de Cliente'] = df_churn['Forma Juridica Original'].apply(map_tipo_cliente)
        df_churn.dropna(subset=['Data de Desinstalacao'], inplace=True)
        df_churn['Ano Churn'] = df_churn['Data de Desinstalacao'].dt.year.astype(int)
        df_churn['Mes Churn'] = df_churn['Data de Desinstalacao'].dt.month.astype(int)

        month_names_map = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                           7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
        df_churn['Nome Mes Churn'] = df_churn['Mes Churn'].map(month_names_map)
        df_churn['Volume'] = 1
        df_churn['AnoMes'] = df_churn['Data de Desinstalacao'].dt.to_period('M').astype(str)

    # df_churn é uma cópia de um subconjunto de df_combined, então 'Categoria4_Motivo' e 'Filial'
    # já devem ser propagadas para df_churn se elas foram criadas em df_combined.

    for col in df_churn.select_dtypes(include=['object']).columns:
        df_churn[col] = df_churn[col].astype(str)

    for col in df_active_processed.select_dtypes(include=['object']).columns:
        df_active_processed[col] = df_active_processed[col].astype(str)

    return df_churn, df_active_processed


# Definição da função map_tipo_cliente
def map_tipo_cliente(forma_juridica):
    s = str(forma_juridica).strip().upper()
    if pd.isna(forma_juridica) or s == '':
        return 'PF'
    elif s == 'P1':
        return 'PME'
    elif s == 'C1':
        return 'Corporativo'
    elif s == 'PF':
        return 'PF'
    elif s == 'PME':
        return 'PME'
    elif s == 'CORPORATIVO':
        return 'Corporativo'
    else:
        return 'Outros'

# --- Função Principal do Aplicativo Streamlit ---
def main():
    st.set_page_config(layout="wide", page_title="Dashboard de Churn")

    st.title("📊 Dashboard de Análise de Churn")
    st.write("Visão geral do volume de churn mensal e por tipo de cliente, com projeções anuais.")

    # --- NOVO: Data da Última Atualização (apenas data) ---
    try:
        script_path = os.path.abspath(__file__)
        last_modified_timestamp = os.path.getmtime(script_path)
        last_modified_dt = datetime.fromtimestamp(last_modified_timestamp)

        # Mapeamento para nomes de meses em português
        portuguese_month_names = {
            1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril", 5: "maio", 6: "junho",
            7: "julho", 8: "agosto", 9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
        }
        
        formatted_date = f"{last_modified_dt.day} de {portuguese_month_names[last_modified_dt.month]} de {last_modified_dt.year}"
        
        st.markdown(f"**Última Atualização:** {formatted_date}")
    except Exception as e:
        st.warning(f"Não foi possível determinar a data da última atualização: {e}")
    # --- FIM: Data da Última Atualização ---


    df_churn, df_active_raw = load_and_transform_data(data_dir, file_2024, file_2025, file_active_base)

    st.sidebar.header("Filtros")

    # Verifica se df_churn é válido antes de tentar acessar colunas
    if df_churn.empty or 'Ano Churn' not in df_churn.columns:
        st.error("ERRO: Dados de CHURN vazios ou incompletos. Verifique os arquivos de origem ou filtros.")
        st.stop()

    # --- Filtro de Anos com "Selecionar Todos" ---
    all_years = sorted(df_churn['Ano Churn'].unique())
    # Adiciona "Todos" como a primeira opção
    display_years = ["Todos"] + all_years
    selected_years_option = st.sidebar.multiselect(
        "Selecione o(s) Ano(s)",
        options=display_years,
        default=["Todos"] # Define "Todos" como padrão
    )
    # Se "Todos" estiver selecionado, usa todos os anos disponíveis
    if "Todos" in selected_years_option:
        selected_years = all_years
    else:
        selected_years = selected_years_option
    # --- Fim do Filtro de Anos ---

    month_order_num_pt = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                          "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    # Adicionado: Lista de meses abreviados para o eixo X
    month_abbr_order_pt = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                           "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    # Mapeamento de nome completo para abreviado
    month_to_abbr_map = dict(zip(month_order_num_pt, month_abbr_order_pt))


    # --- Filtro de Meses com "Selecionar Todos" ---
    all_months = sorted(df_churn['Nome Mes Churn'].unique(), key=lambda x: month_order_num_pt.index(x))
    display_months = ["Todos"] + all_months
    selected_months_option = st.sidebar.multiselect(
        "Selecione o(s) Mês(es)",
        options=display_months,
        default=["Todos"] # Define "Todos" como padrão
    )
    if "Todos" in selected_months_option:
        selected_months = all_months
    else:
        selected_months = selected_months_option
    # --- Fim do Filtro de Meses ---

    # --- Filtro de Tipo de Cliente com "Selecionar Todos" ---
    all_client_types = df_churn['Tipo de Cliente'].unique()
    display_client_types = ["Todos"] + list(all_client_types)
    selected_client_types_option = st.sidebar.multiselect(
        "Selecione o(s) Tipo(s) de Cliente",
        options=display_client_types,
        default=["Todos"] # Define "Todos" como padrão
    )
    if "Todos" in selected_client_types_option:
        selected_client_types = list(all_client_types) # Convertendo para lista explicitamente
    else:
        selected_client_types = selected_client_types_option
    # --- Fim do Filtro de Tipo de Cliente ---


    if 'Tipo de Churn' in df_churn.columns and not df_churn['Tipo de Churn'].isnull().all():
        # --- Filtro de Tipo de Churn com "Selecionar Todos" ---
        all_churn_types = df_churn['Tipo de Churn'].unique()
        display_churn_types = ["Todos"] + list(all_churn_types)
        selected_churn_types_option = st.sidebar.multiselect(
            "Selecione o(s) Tipo(s) de Churn",
            options=display_churn_types,
            default=["Todos"] # Define "Todos" como padrão
        )
        if "Todos" in selected_churn_types_option:
            selected_churn_types = list(all_churn_types) # Convertendo para lista explicitamente
        else:
            selected_churn_types = selected_churn_types_option
        # --- Fim do Filtro de Tipo de Churn ---
    else:
        selected_churn_types = all_churn_types = []


    df_filtered = df_churn[
        (df_churn['Ano Churn'].isin(selected_years)) &
        (df_churn['Nome Mes Churn'].isin(selected_months)) &
        (df_churn['Tipo de Cliente'].isin(selected_client_types))
    ]
    if 'Tipo de Churn' in df_filtered.columns and selected_churn_types:
        df_filtered = df_filtered[df_filtered['Tipo de Churn'].isin(selected_churn_types)]


    # --- IMPORTANTE: Se df_filtered estiver vazio, o st.stop() é ativado aqui ---
    if df_filtered.empty:
        st.warning("Nenhum dado de CHURN encontrado com os filtros selecionados. Ajuste os filtros na barra lateral.")
        st.stop() # Esta linha interrompe a execução do script se não houver dados.

    # --- INÍCIO DA SEÇÃO DE KPIS ---
    st.header("Indicadores de Performance")
    # Envolve os KPIs em um container para layout isolado
    with st.container(): 
        # Crie 6 colunas para os KPIs
        # Reorganizando as colunas: 1, 2, 3 (Projeção), 6 (Churn Rate), 4 (Base Ativa), 5 (Média Var. Churn)
        col1, col2, col3_proj, col6_churn_rate, col4_base_ativa, col5_media_var = st.columns(6)

        with col1:
            with st.container(border=True): # Caixa para o KPI
                st.markdown("<h5 style='text-align: center;'>Total de Churn (2025)</h5>", unsafe_allow_html=True)
                df_churn_2025_kpi = df_churn[df_churn['Ano Churn'] == 2025].copy()
                df_churn_2025_kpi = df_churn_2025_kpi[
                    (df_churn_2025_kpi['Nome Mes Churn'].isin(selected_months)) &
                    (df_churn_2025_kpi['Tipo de Cliente'].isin(selected_client_types))
                ]
                if 'Tipo de Churn' in df_churn_2025_kpi.columns and selected_churn_types:
                    df_churn_2025_kpi = df_churn_2025_kpi[df_churn_2025_kpi['Tipo de Churn'].isin(selected_churn_types)]

                total_churn_2025_only = df_churn_2025_kpi['Volume'].sum()
                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    # Ajustado para h5 para um tamanho de fonte menor
                    st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{total_churn_2025_only:,.0f}</h5>".replace(",", "."), unsafe_allow_html=True)

        with col2:
            with st.container(border=True): # Caixa para o KPI
                st.markdown(f"<h5 style='text-align: center;'>Variação Mensal (Absoluta)</h5>", unsafe_allow_html=True) 
                delta_value_for_display = None
                if not df_filtered.empty:
                    current_month_churn_2025 = 0
                    current_month_churn_2024 = 0
                    if selected_months:
                        last_selected_month_name = selected_months[-1]
                        last_selected_month_num = month_order_num_pt.index(last_selected_month_name) + 1
                        
                        temp_df_churn_2025 = df_churn[
                            (df_churn['Ano Churn'] == 2025) &
                            (df_churn['Mes Churn'] == last_selected_month_num) &
                            (df_churn['Tipo de Cliente'].isin(selected_client_types))
                        ]
                        if selected_churn_types:
                            temp_df_churn_2025 = temp_df_churn_2025[temp_df_churn_2025['Tipo de Churn'].isin(selected_churn_types)]
                        current_month_churn_2025 = temp_df_churn_2025['Volume'].sum()

                        temp_df_churn_2024 = df_churn[
                            (df_churn['Ano Churn'] == 2024) &
                            (df_churn['Mes Churn'] == last_selected_month_num) &
                            (df_churn['Tipo de Cliente'].isin(selected_client_types))
                        ]
                        if selected_churn_types:
                            temp_df_churn_2024 = temp_df_churn_2024[temp_df_churn_2024['Tipo de Churn'].isin(selected_churn_types)]
                        current_month_churn_2024 = temp_df_churn_2024['Volume'].sum()
                        
                        delta_value_for_display = current_month_churn_2025 - current_month_churn_2024
                
                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    if delta_value_for_display is not None:
                        # Ajustado para h5 para um tamanho de fonte menor e font-weight: normal
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{delta_value_for_display:,.0f}</h5>".replace(",", "."), unsafe_allow_html=True)
                    else:
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>N/A</h5>", unsafe_allow_html=True)

        with col3_proj: # Coluna para Projeção Anual Churn
            with st.container(border=True): # Caixa para o KPI
                st.markdown(f"<h5 style='text-align: center;'>Projeção Anual Churn ({max(selected_years) if selected_years else 'N/A'})</h5>", unsafe_allow_html=True)
                projected_annual_churn = 0 # Inicializa para garantir que a variável exista
                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    if selected_years:
                        current_year_churn_proj = max(selected_years)
                        df_current_year_churn = df_filtered[df_filtered['Ano Churn'] == current_year_churn_proj]

                        if not df_current_year_churn.empty:
                            max_month_data_churn = df_current_year_churn['Mes Churn'].max()
                            churn_accumulated = df_current_year_churn[df_current_year_churn['Mes Churn'] <= max_month_data_churn]['Volume'].sum()
                            num_months_data_churn = df_current_year_churn['Mes Churn'].nunique()

                            if num_months_data_churn > 0:
                                projected_annual_churn = (churn_accumulated / num_months_data_churn) * 12
                                # Ajustado para h5 para um tamanho de fonte menor e font-weight: normal
                                st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{int(projected_annual_churn):,.0f}</h5>".replace(",", "."), unsafe_allow_html=True)
                            else:
                                st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>Sem dados para projeção</h5>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>N/A</h5>", unsafe_allow_html=True)
                    else:
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>Selecione um ano para projetar.</h5>", unsafe_allow_html=True)

        with col6_churn_rate: # Nova coluna para Churn Rate (agora ao lado da projeção)
            with st.container(border=True): # Caixa para o KPI
                st.markdown("<h5 style='text-align: center;'>Churn Rate Anual (%)</h5>", unsafe_allow_html=True)
                churn_rate_value = "N/A"
                
                # Recalculando projected_annual_churn_calc e avg_monthly_active_calc para garantir acesso global
                projected_annual_churn_calc = 0
                avg_monthly_active_calc = 0

                # Lógica para projected_annual_churn_calc
                if selected_years:
                    current_year_churn_proj_calc = max(selected_years)
                    df_current_year_churn_calc = df_churn[ 
                        (df_churn['Ano Churn'] == current_year_churn_proj_calc) &
                        (df_churn['Nome Mes Churn'].isin(selected_months)) &
                        (df_churn['Tipo de Cliente'].isin(selected_client_types))
                    ]
                    if selected_churn_types:
                            df_current_year_churn_calc = df_current_year_churn_calc[df_current_year_churn_calc['Tipo de Churn'].isin(selected_churn_types)]

                    if not df_current_year_churn_calc.empty:
                        max_month_data_churn_calc = df_current_year_churn_calc['Mes Churn'].max()
                        churn_accumulated_calc = df_current_year_churn_calc[df_current_year_churn_calc['Mes Churn'] <= max_month_data_churn_calc]['Volume'].sum()
                        num_months_data_churn_calc = df_current_year_churn_calc['Mes Churn'].nunique()
                        if num_months_data_churn_calc > 0:
                            projected_annual_churn_calc = (churn_accumulated_calc / num_months_data_churn_calc) * 12

                # Lógica para avg_monthly_active_calc
                if not df_active_raw.empty:
                    current_year_active_proj_calc = 2025 
                    df_current_year_active_filtered_calc = df_active_raw[
                        (df_active_raw['Mes Base Ativa'].isin([month_order_num_pt.index(m)+1 for m in selected_months])) &
                        (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
                    ].copy()
                    if 'Tipo de Cliente Base Ativa' in df_current_year_active_filtered_calc.columns and selected_client_types:
                        df_current_year_active_filtered_calc = df_current_year_active_filtered_calc[df_current_year_active_filtered_calc['Tipo de Cliente Base Ativa'].isin(selected_client_types)]
                    
                    if not df_current_year_active_filtered_calc.empty:
                        total_active_until_now_calc = df_current_year_active_filtered_calc['Volume Base Ativa'].sum()
                        num_months_active_data_calc = df_current_year_active_filtered_calc['Mes Base Ativa'].nunique()
                        if num_months_active_data_calc > 0:
                            avg_monthly_active_calc = total_active_until_now_calc / num_months_active_data_calc

                if projected_annual_churn_calc is not None and avg_monthly_active_calc is not None and avg_monthly_active_calc > 0:
                    churn_rate_value = (projected_annual_churn_calc / avg_monthly_active_calc) * 100 
                
                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    if isinstance(churn_rate_value, (int, float)):
                        # Ajustado para h5 para um tamanho de fonte menor e font-weight: normal
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{churn_rate_value:.2f}%</h5>", unsafe_allow_html=True)
                    else:
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{churn_rate_value}</h5>", unsafe_allow_html=True)
        
        with col4_base_ativa: # Coluna para Média Mensal Base Ativa (agora é a 5ª visualmente)
            with st.container(border=True): # Caixa para o KPI
                st.markdown(f"<h5 style='text-align: center;'>Média Mensal Base Ativa</h5>", unsafe_allow_html=True)
                # avg_monthly_active_calc já foi calculado acima no escopo global para o Churn Rate
                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    if avg_monthly_active_calc > 0: # Usar avg_monthly_active_calc do escopo de cálculo
                        # Ajustado para h5 para um tamanho de fonte menor e font-weight: normal
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{int(avg_monthly_active_calc):,.0f}</h5>".replace(",", "."), unsafe_allow_html=True,
                                     help="Os dados neste KPI referem-se ao ano de 2025.")
                    else:
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>N/A</h5>", unsafe_allow_html=True, help="Arquivo de base ativa não carregado ou vazio.")

        with col5_media_var: # Coluna para Churn 2025 vs 2024 (agora é a 6ª visualmente)
            with st.container(border=True): # Caixa para o KPI
                st.markdown("<h5 style='text-align: center;'>Variação Churn vs 2024</h5>", unsafe_allow_html=True) # ATUALIZADO: Nome do KPI
                # Passo 1: Preparar os dados para o cálculo do KPI aplicando os filtros de Tipo de Cliente e Tipo de Churn
                df_churn_for_kpi_comparison = df_churn.copy()
                if selected_client_types:
                    df_churn_for_kpi_comparison = df_churn_for_kpi_comparison[df_churn_for_kpi_comparison['Tipo de Cliente'].isin(selected_client_types)]
                if 'Tipo de Churn' in df_churn_for_kpi_comparison.columns and selected_churn_types:
                    df_churn_for_kpi_comparison = df_churn_for_kpi_comparison[df_churn_for_kpi_comparison['Tipo de Churn'].isin(selected_churn_types)]

                # Passo 2: Agrupar por Ano e Mês para obter o volume mensal, apenas para os anos de 2024 e 2025
                df_monthly_volumes_kpi = df_churn_for_kpi_comparison[
                    df_churn_for_kpi_comparison['Ano Churn'].isin([2024, 2025])
                ].groupby(['Ano Churn', 'Mes Churn']).agg(
                    Volume_Churn=('Volume', 'sum')
                ).reset_index()

                # Passo 3: Pivotar para ter 2024 e 2025 como colunas
                df_comparison = df_monthly_volumes_kpi.pivot_table(
                    index='Mes Churn',
                    columns='Ano Churn',
                    values='Volume_Churn'
                ).reset_index()

                # Passo 4: Filtrar pelos meses selecionados na sidebar
                selected_month_nums = [month_order_num_pt.index(m)+1 for m in selected_months]
                df_comparison_filtered_months = df_comparison[df_comparison['Mes Churn'].isin(selected_month_nums)].copy()

                # Garantir que as colunas 2024 e 2025 existam e preencher NaN com 0
                df_comparison_filtered_months[2024] = df_comparison_filtered_months.get(2024, pd.Series(0, index=df_comparison_filtered_months.index)).fillna(0)
                df_comparison_filtered_months[2025] = df_comparison_filtered_months.get(2025, pd.Series(0, index=df_comparison_filtered_months.index)).fillna(0)

                # Passo 5: Calcular a variação percentual para cada mês onde há dados de 2024 para evitar divisão por zero
                df_comparison_filtered_months['Monthly_Variation'] = pd.NA
                
                # Calcular a variação apenas onde o churn de 2024 não é zero
                valid_comparison_rows = df_comparison_filtered_months[df_comparison_filtered_months[2024] > 0]
                if not valid_comparison_rows.empty:
                    df_comparison_filtered_months.loc[valid_comparison_rows.index, 'Monthly_Variation'] = \
                        (valid_comparison_rows[2025] - valid_comparison_rows[2024]) / valid_comparison_rows[2024]

                # Passo 6: Calcular a média das variações mensais
                average_monthly_variation = df_comparison_filtered_months['Monthly_Variation'].mean()

                kpi_col_inner_left, kpi_col_inner_center, kpi_col_inner_right = st.columns([0.1, 1, 0.1])
                with kpi_col_inner_center:
                    if pd.notna(average_monthly_variation):
                        # Seta e cor (verde para redução, vermelho para aumento)
                        delta_value_for_arrow = abs(average_monthly_variation)
                        delta_color_option = "inverse" if average_monthly_variation < 0 else "normal" # Verde se negativo, vermelho se positivo

                        st.markdown(
                            f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>{average_monthly_variation:.2%}</h5>",
                            unsafe_allow_html=True,
                            help="Média das variações percentuais mensais do churn de 2025 em relação a 2024, para os meses selecionados."
                        )
                    else:
                        st.markdown(f"<h5 style='text-align: center; color: #31333F; font-weight: normal;'>N/A</h5>", unsafe_allow_html=True, help="Dados insuficientes para calcular a média das variações mensais (verifique se há dados para ambos os anos nos meses selecionados).")
    # --- FIM DA SEÇÃO DE KPIS ---
    st.markdown("---") # Separador após a seção de KPIs

    # --- GRÁFICO DE BARRAS: Volume Mensal de Churn COM Variação Ano-sobre-Ano no Eixo X ---
    st.header("Churn Mensal por Ano e Variação")

    # --- NOVO: Preparar dados da Base Ativa para o gráfico de Churn Rate Mensal ---
    df_active_monthly_volumes = pd.DataFrame()
    if not df_active_raw.empty:
        # Filtrar df_active_raw pelos meses e tipos de cliente selecionados na sidebar
        df_active_filtered_for_chart = df_active_raw[
            (df_active_raw['Mes Base Ativa'].isin([month_order_num_pt.index(m)+1 for m in selected_months])) &
            (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
        ].copy()
        
        # REMOVIDO: Bloco de verificação e warning para 2024
        
        df_active_monthly_volumes = df_active_filtered_for_chart.groupby(['Ano Base Ativa', 'Mes Base Ativa', 'Nome Mes Ativa']).agg(
            Volume_Base_Ativa=('Volume Base Ativa', 'sum')
        ).reset_index()
        
        # Renomear colunas para facilitar o merge com df_plot_monthly_volume
        df_active_monthly_volumes.rename(columns={
            'Ano Base Ativa': 'Ano Churn',
            'Mes Base Ativa': 'Mes Churn',
            'Nome Mes Ativa': 'Nome Mes Churn'
        }, inplace=True)
    # REMOVIDO: else block with warning for empty df_active_raw
    # --- FIM: Preparar dados da Base Ativa ---


    # df_plot_monthly_volume já está pronto do código existente
    df_plot_monthly_volume = df_filtered.groupby(['Ano Churn', 'Mes Churn', 'Nome Mes Churn']).agg(
        Volume_Churn=('Volume', 'sum')
    ).reset_index().sort_values(by=['Ano Churn', 'Mes Churn'])

    # --- NOVO: Calcular Churn Rate Mensal e adicionar ao df_plot_monthly_volume ---
    # Merge de dados de churn com dados da base ativa
    df_plot_monthly_volume = pd.merge(
        df_plot_monthly_volume,
        df_active_monthly_volumes,
        on=['Ano Churn', 'Mes Churn', 'Nome Mes Churn'],
        how='left'
    )
    
    # Calcular Churn Rate. Lidar com divisão por zero e formatar para o label.
    df_plot_monthly_volume['Churn_Rate'] = df_plot_monthly_volume.apply(
        lambda row: (row['Volume_Churn'] / row['Volume_Base_Ativa'] * 100) if row['Volume_Base_Ativa'] > 0 else float('nan'), # Usar NaN para indicar "N/A"
        axis=1
    )
    
    # Criar o label de texto para o gráfico (condicional por ano)
    df_plot_monthly_volume['Bar_Text_Label'] = df_plot_monthly_volume.apply(
        lambda row: (
            f"{row['Volume_Churn']:,.0f}".replace(",", ".") +  # Sempre mostrar volume absoluto
            (f"<br>{row['Churn_Rate']:.2f}%" if pd.notna(row['Churn_Rate']) and row['Ano Churn'] == 2025 else "") # Adicionar % apenas para 2025 se disponível
        ),
        axis=1
    )
    # --- FIM: Calcular Churn Rate Mensal ---


    df_yoy_comparison = df_plot_monthly_volume.pivot_table(
        index=['Mes Churn', 'Nome Mes Churn'],
        columns='Ano Churn',
        values='Volume_Churn'
    ).reset_index()

    if 2024 in df_yoy_comparison.columns and 2025 in df_yoy_comparison.columns:
        df_yoy_comparison['YoY_Variation'] = (
            (df_yoy_comparison[2025] - df_yoy_comparison[2024]) /
            df_yoy_comparison[2024].replace(0, pd.NA)
        ).fillna(pd.NA)
    else:
        df_yoy_comparison['YoY_Variation'] = pd.NA

    df_yoy_pivot_for_label = df_yoy_comparison.copy()
    df_yoy_pivot_for_label['X_Axis_Month_Label'] = df_yoy_pivot_for_label.apply(
        lambda row: (
            f"{row['Nome Mes Churn']}" +
            (f"<br>({row['YoY_Variation']:.1%})" if pd.notna(row['YoY_Variation']) else "")
        ),
        axis=1
    )

    df_plot_monthly = pd.merge(
        df_plot_monthly_volume,
        df_yoy_pivot_for_label[['Mes Churn', 'X_Axis_Month_Label', 'YoY_Variation']],
        on='Mes Churn',
        how='left'
    )

    # df_plot_monthly['Bar_Text_Label'] já está definido acima com o Churn Rate ou Volume

    fig_monthly_bar_with_variation = px.bar(
        df_plot_monthly,
        x="X_Axis_Month_Label",
        y="Volume_Churn",
        color=df_plot_monthly['Ano Churn'].astype(str),
        barmode="group",
        labels={
            "X_Axis_Month_Label": "Mês (Variação YoY)",
            # REMOVIDO: "Volume_Churn": "Volume de Churn",
            "color": "Ano"
        },
        category_orders={"X_Axis_Month_Label": sorted(df_yoy_pivot_for_label['X_Axis_Month_Label'].unique(), key=lambda x: month_order_num_pt.index(x.split('<br>')[0]) if '<br>' in x else month_order_num_pt.index(x))},
        text='Bar_Text_Label' # Usa o novo label com Churn Rate
    )

    fig_monthly_bar_with_variation.update_traces(
        textposition='outside', # Fora da barra para o Churn Rate
        textfont=dict(color='black', weight='bold', size=10), # Reduzindo o tamanho da fonte para duas linhas
        textangle=0 # Horizontal para melhor leitura
    )

    fig_monthly_bar_with_variation.update_traces(
        hovertemplate="<b>Mês:</b> %{customdata[1]}<br><b>Ano:</b> %{fullData.name}<br><b>Volume:</b> %{y:,.0f}" +
                      "<br><b>Variação (25 vs 24):</b> %{customdata[0]:.1%}<extra></extra>" +
                      "<br><b>Informação na barra:</b> %{text}<extra></extra>", # Hover que mostra o conteúdo da label
        customdata=df_plot_monthly[['YoY_Variation', 'Nome Mes Churn']]
    )

    fig_monthly_bar_with_variation.update_layout(
        hovermode="x unified",
        yaxis_title="", # REMOVIDO: Título do eixo Y
        legend=dict( # ATUALIZADO: Legenda para CIMA e DIREITA para este gráfico
            font=dict(
                size=12,
                color="black",
                family="Arial",
                weight="bold"
            ),
            orientation="v", # Vertical
            yanchor="top",   # Ancorado na parte SUPERIOR
            y=1,             # Posição Y (alinha com o topo do gráfico)
            xanchor="right", # Ancorado na DIREITA
            x=1.1            # Posição X (fora da área do gráfico)
        ),
        xaxis_title="Mês (Variação YoY)",
        xaxis=dict(tickangle=0)
    )
    st.plotly_chart(fig_monthly_bar_with_variation, use_container_width=True)


    st.header("Distribuição de Churn por Tipo de Cliente")

    # RESTAURADO: Seção original com gráficos de pizza e variação por tipo de cliente
    # Filtra os dados para 2024 e 2025 (considerando os filtros de mês e tipo de cliente)
    df_churn_2024 = df_churn[
        (df_churn['Ano Churn'] == 2024) &
        (df_churn['Nome Mes Churn'].isin(selected_months)) &
        (df_churn['Tipo de Cliente'].isin(selected_client_types))
    ]
    if selected_churn_types:
        df_churn_2024 = df_churn_2024[df_churn_2024['Tipo de Churn'].isin(selected_churn_types)]


    df_churn_2025 = df_churn[
        (df_churn['Ano Churn'] == 2025) &
        (df_churn['Nome Mes Churn'].isin(selected_months)) &
        (df_churn['Tipo de Cliente'].isin(selected_client_types))
    ]
    if selected_churn_types:
        df_churn_2025 = df_churn_2025[df_churn_2025['Tipo de Churn'].isin(selected_churn_types)]


    # Agrupa por Tipo de Cliente para ambos os anos
    df_plot_client_type_2024 = df_churn_2024.groupby('Tipo de Cliente').agg(
        Volume_Churn=('Volume', 'sum')
    ).reset_index().sort_values(by='Volume_Churn', ascending=False)

    df_plot_client_type_2025 = df_churn_2025.groupby('Tipo de Cliente').agg(
        Volume_Churn=('Volume', 'sum')
    ).reset_index().sort_values(by='Volume_Churn', ascending=False)

    # Cria colunas para os gráficos e a seção de comparação
    col_2024, col_2025, col_comparison = st.columns([1, 1, 1]) 

    with col_2024:
        st.markdown("<h3 style='text-align: center;'>Consolidado 2024</h3>", unsafe_allow_html=True) # CENTRALIZADO
        if not df_plot_client_type_2024.empty:
            fig_client_type_2024 = px.pie(
                df_plot_client_type_2024,
                values="Volume_Churn",
                names="Tipo de Cliente",
                hole=0.4
            )
            # ATUALIZADO: Legenda para baixo para 2024
            fig_client_type_2024.update_traces(textinfo="percent+label", pull=[0.05]*len(df_plot_client_type_2024))
            fig_client_type_2024.update_layout(
                showlegend=True, # Garante que a legenda é mostrada
                legend=dict(
                    orientation="h", # Horizontal
                    yanchor="bottom", # Ancorado na parte inferior
                    y=-0.2, # Posição Y (ajuste conforme necessário)
                    xanchor="center", # Ancorado no centro
                    x=0.5, # Posição X
                    font=dict(
                        size=12,
                        color="black",
                        family="Arial",
                        weight="bold"
                    )
                )
            )
            st.plotly_chart(fig_client_type_2024, use_container_width=True)
        else:
            st.info("Nenhum dado de churn para 2024 com os filtros selecionados.")

    with col_2025:
        st.markdown("<h3 style='text-align: center;'>Consolidado 2025</h3>", unsafe_allow_html=True) # CENTRALIZADO
        if not df_plot_client_type_2025.empty:
            fig_client_type_2025 = px.pie(
                df_plot_client_type_2025,
                values="Volume_Churn",
                names="Tipo de Cliente",
                hole=0.4
            )
            # ATUALIZADO: Legenda para baixo para 2025
            fig_client_type_2025.update_traces(textinfo="percent+label", pull=[0.05]*len(df_plot_client_type_2025))
            fig_client_type_2025.update_layout(
                legend=dict(
                    font=dict(
                        size=12,
                        color="black",
                        family="Arial",
                        weight="bold"
                    )
                )
            )
            st.plotly_chart(fig_client_type_2025, use_container_width=True)
        else:
            st.info("Nenhum dado de churn para 2025 com os filtros selecionados.")

    with col_comparison:
        st.subheader("Variação Anual (2025 vs 2024)")
        df_comparison_client_type = pd.merge(
            df_plot_client_type_2024.rename(columns={'Volume_Churn': 'Volume_2024'}),
            df_plot_client_type_2025.rename(columns={'Volume_Churn': 'Volume_2025'}),
            on='Tipo de Cliente',
            how='outer'
        ).fillna(0) # Preenche NaN com 0 para tipos de cliente que não apareceram em um dos anos

        df_comparison_client_type['Diferenca_Absoluta'] = df_comparison_client_type['Volume_2025'] - df_comparison_client_type['Volume_2024']
        
        # Evitar divisão por zero para cálculo de %
        df_comparison_client_type['Diferenca_Percentual'] = df_comparison_client_type.apply(
            lambda row: ((row['Volume_2025'] - row['Volume_2024']) / row['Volume_2024']) * 100 if row['Volume_2024'] != 0 else (100 if row['Volume_2025'] > 0 else 0),
            axis=1
        )
        
        if not df_comparison_client_type.empty:
            with st.container(border=True): # Container principal para todas as métricas de variação
                st.markdown("<h4 style='text-align: center; color: gray;'>Diferenças por Tipo de Cliente</h4>", unsafe_allow_html=True)
                
                for index, row in df_comparison_client_type.iterrows():
                    tipo_cliente = row['Tipo de Cliente']
                    diff_abs = row['Diferenca_Absoluta']
                    diff_perc = row['Diferenca_Percentual']
                                        
                    delta_color = "normal" if diff_abs < 0 else "inverse" 

                    # Cria 3 colunas para cada metric: esquerda vazia, metric, direita vazia
                    col_left_spacer_inner, col_metric_content_inner, col_right_spacer_inner = st.columns([0.2, 0.6, 0.2])
                    
                    with col_metric_content_inner:
                        st.metric(
                            label=f"**{tipo_cliente}**",
                            value=f"{int(diff_abs):,.0f}".replace(",", "."),
                            delta=f"{diff_perc:.1f}%",
                            delta_color=delta_color
                        )
        else:
            st.info("Nenhuma variação para exibir com os filtros selecionados.")
    # FIM DA SEÇÃO RESTAURADA

    # --- NOVA SEÇÃO: Volume de Churn Mensal por Tipo (GRÁFICO DE BARRAS EMPILHADAS) ---
    st.markdown("---")
    st.header("Volume de Churn Mensal por Tipo") # Título do gráfico
    
    if 'Tipo de Churn' in df_filtered.columns and not df_filtered['Tipo de Churn'].isnull().all():
        # Agrupa os dados filtrados por Ano, Mês e Tipo de Churn para o gráfico empilhado
        df_plot_churn_type_monthly = df_filtered.groupby(['Ano Churn', 'Mes Churn', 'Nome Mes Churn', 'Tipo de Churn']).agg(
            Volume_Churn=('Volume', 'sum')
        ).reset_index()

        # Abreviar nomes dos meses para o eixo X
        df_plot_churn_type_monthly['Nome Mes Abreviado'] = df_plot_churn_type_monthly['Nome Mes Churn'].map(month_to_abbr_map)

        # Ordena os meses abreviados corretamente para o eixo X
        ordered_abbr_months = month_abbr_order_pt
        
        fig_churn_type_monthly_stacked = px.bar(
            df_plot_churn_type_monthly,
            x="Nome Mes Abreviado", # Mês abreviado no eixo X
            y="Volume_Churn",
            color="Tipo de Churn", # Empilha por Tipo de Churn
            facet_col="Ano Churn", # Cria colunas separadas para 2024 e 2025
            barmode="stack", # Define o modo de empilhamento
            labels={
                "Nome Mes Abreviado": "Mês", # Label atualizado para mês abreviado
                "Volume_Churn": "Volume de Churn",
                "Ano Churn": "Ano",
                "Tipo de Churn": "Tipo de Churn"
            },
            category_orders={"Nome Mes Abreviado": ordered_abbr_months}, # Ordem dos meses abreviados
            text_auto=True # Automaticamente mostra os valores nas barras
        )
        fig_churn_type_monthly_stacked.update_traces(
            textposition='inside', # Posição do texto dentro da barra
            textfont=dict(color='white', weight='bold', size=12),
            textangle=0 # Mantém o texto horizontal
        )
        fig_churn_type_monthly_stacked.update_layout(
            title="", # Removido o título do layout
            xaxis_title="Mês",
            yaxis_title="Volume de Churn",
            legend=dict(
                font=dict(
                    size=12,
                    color="black",
                    family="Arial",
                    weight="bold"
                ),
                title_text="", # Título da legenda
                orientation="h", # Legenda horizontal
                yanchor="bottom", # Ancorado na parte inferior
                y=-0.25, # Posição Y (ajuste conforme necessário para o espaçamento)
                xanchor="center", # Ancorado no centro
                x=0.5 # Posição X
            ),
            hovermode="x unified" # Melhora o hover para barras agrupadas/empilhadas
        )
        # Ajusta os títulos dos subplots (facet_col) para serem mais limpos
        fig_churn_type_monthly_stacked.for_each_annotation(lambda a: a.update(text=a.text.replace("Ano Churn=", "")))

        st.plotly_chart(fig_churn_type_monthly_stacked, use_container_width=True)
    else:
        st.warning("Não há dados de 'Tipo de Churn' para exibir o gráfico mensal empilhado.")
    # --- FIM DA NOVA SEÇÃO: Volume de Churn Mensal por Tipo (GRÁFICO DE BARRAS EMPILHADAS) ---


    # --- NOVA SEÇÃO: Análise de Motivos de Cancelamento por Ano (da Coluna Categoria4) ---
    st.markdown("---")
    st.header("Análise de Motivos de Cancelamento por Ano") # Título atualizado para a comparação

    # --- Data para motivos de cancelamento de 2025 ---
    df_cancellation_analysis_2025_filtered = df_filtered[df_filtered['Ano Churn'] == 2025].copy()
    
    reasons_summary_2025 = pd.DataFrame()
    if 'Categoria4_Motivo' in df_cancellation_analysis_2025_filtered.columns and not df_cancellation_analysis_2025_filtered['Categoria4_Motivo'].isnull().all():
        df_cancellation_analysis_2025_clean = df_cancellation_analysis_2025_filtered.copy()
        df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] = df_cancellation_analysis_2025_clean['Categoria4_Motivo'].astype(str).str.strip().str.lower()
        df_cancellation_analysis_2025_clean = df_cancellation_analysis_2025_clean[
            (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != '') &
            (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != 'nan') & # Remove explicitamente a string 'nan'
            (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != 'desconsiderar')
        ].copy()

        if not df_cancellation_analysis_2025_clean.empty:
            reasons_summary_2025 = df_cancellation_analysis_2025_clean.groupby('Categoria4_Motivo').agg(
                Volume_2025=('Volume', 'sum')
            ).reset_index()
    
    # --- Data para motivos de cancelamento de 2024 ---
    df_cancellation_analysis_2024_filtered = df_filtered[df_filtered['Ano Churn'] == 2024].copy()
    
    reasons_summary_2024 = pd.DataFrame()
    if 'Categoria4_Motivo' in df_cancellation_analysis_2024_filtered.columns and not df_cancellation_analysis_2024_filtered['Categoria4_Motivo'].isnull().all():
        df_cancellation_analysis_2024_clean = df_cancellation_analysis_2024_filtered.copy()
        df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] = df_cancellation_analysis_2024_clean['Categoria4_Motivo'].astype(str).str.strip().str.lower()
        df_cancellation_analysis_2024_clean = df_cancellation_analysis_2024_clean[
            (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != '') &
            (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != 'nan') & # Remove explicitamente a string 'nan'
            (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != 'desconsiderar')
        ].copy()

        if not df_cancellation_analysis_2024_clean.empty:
            reasons_summary_2024 = df_cancellation_analysis_2024_clean.groupby('Categoria4_Motivo').agg(
                Volume_2024=('Volume', 'sum')
            ).reset_index()

    # --- Combinar e Calcular Variação ---
    if not reasons_summary_2025.empty or not reasons_summary_2024.empty:
        df_combined_reasons = pd.merge(
            reasons_summary_2025,
            reasons_summary_2024,
            on='Categoria4_Motivo',
            how='outer'
        ).fillna(0) # Preenche NaN volumes com 0 para cálculo

        # Calcular totais anuais para os percentuais
        df_combined_reasons['Volume_2025_Total'] = df_combined_reasons['Volume_2025'].sum()
        df_combined_reasons['Volume_2024_Total'] = df_combined_reasons['Volume_2024'].sum()

        # Calcular Percentual 2025
        df_combined_reasons['Percentual_2025'] = df_combined_reasons.apply(
            lambda row: (row['Volume_2025'] / row['Volume_2025_Total']) * 100 if row['Volume_2025_Total'] > 0 else 0, axis=1
        )
        
        # Calcular Percentual 2024
        df_combined_reasons['Percentual_2024'] = df_combined_reasons.apply(
            lambda row: (row['Volume_2024'] / row['Volume_2024_Total']) * 100 if row['Volume_2024_Total'] > 0 else 0, axis=1
        )

        # Calcular Variação 2025 vs 2024 (RAZÃO, formatada como %)
        df_combined_reasons['Variação 2025 vs 2024'] = df_combined_reasons.apply(
            lambda row: (
                ((row['Volume_2025'] / row['Volume_2024']) - 1) # Calcula a diferença percentual
                if row['Volume_2024'] > 0 else (
                    float('inf') if row['Volume_2025'] > 0 else 0 # Infinito se novo motivo, 0 se ambos são 0
                )
            ),
            axis=1
        )
        
        # Renomear para exibição e formatar
        df_combined_reasons_display = df_combined_reasons.copy() # Criar uma cópia para renomear e formatar
        df_combined_reasons_display['Volume_2025'] = df_combined_reasons_display['Volume_2025'].astype(int)
        df_combined_reasons_display['Volume_2024'] = df_combined_reasons_display['Volume_2024'].astype(int)
        df_combined_reasons_display['Percentual_2025'] = df_combined_reasons_display['Percentual_2025'].map('{:.2f}%'.format)
        df_combined_reasons_display['Percentual_2024'] = df_combined_reasons_display['Percentual_2024'].map('{:.2f}%'.format)
        
        # Formato customizado para a coluna de variação (X.YY%) com vírgula decimal
        df_combined_reasons_display['Variação 2025 vs 2024'] = df_combined_reasons_display['Variação 2025 vs 2024'].apply(
            lambda x: f"{x:.2f}%".replace('.', ',') if pd.notna(x) and x != float('inf') else ("Novo Motivo" if x == float('inf') else "0,00%")
        )
        
        # Renomear as colunas para o display final
        df_combined_reasons_display.rename(columns={
            'Categoria4_Motivo': 'Motivo de Cancelamento', # Coluna original que virou 'Motivo de Cancelamento'
            'Volume_2025': 'Volume 2025',
            'Percentual_2025': '% 2025',
            'Volume_2024': 'Volume 2024',
            'Percentual_2024': '% 2024'
        }, inplace=True)

        # Reordenar colunas para exibição (após renomear)
        df_combined_reasons_display = df_combined_reasons_display[[
            'Motivo de Cancelamento', 'Volume 2025', '% 2025',
            'Volume 2024', '% 2024', 'Variação 2025 vs 2024'
        ]]

        # st.subheader("Tabela de Motivos de Cancelamento (Variação Anual)") # REMOVIDO: Título do subheader
        st.dataframe(df_combined_reasons_display, use_container_width=True, hide_index=True) # Esconde o índice
    else:
        st.info("Nenhum dado de motivos de cancelamento (da Categoria4) encontrado para 2024 ou 2025 com os filtros selecionados, ou todos foram 'Desconsiderar' / vazios.")
    # --- FIM DA NOVA SEÇÃO ---


    # --- NOVA SEÇÃO: Análise de Churn por Filial por Ano ---
    st.markdown("---")
    st.header("Análise de Churn por Filial por Ano")

    # --- Data para análise de filial de 2025 ---
    df_franchise_analysis_2025_filtered = df_filtered[df_filtered['Ano Churn'] == 2025].copy()
    
    reasons_summary_franchise_2025 = pd.DataFrame()
    if 'Filial' in df_franchise_analysis_2025_filtered.columns and not df_franchise_analysis_2025_filtered['Filial'].isnull().all():
        df_franchise_analysis_2025_clean = df_franchise_analysis_2025_filtered.copy()
        df_franchise_analysis_2025_clean['Filial_Lower'] = df_franchise_analysis_2025_clean['Filial'].astype(str).str.strip().str.lower()
        # Adiciona lógica de 'desconsiderar' se aplicável a Filial (assumindo que não há 'desconsiderar' em Filial por padrão)
        # Se 'desconsiderar' puder ocorrer em Filial, adicione aqui:
        # df_franchise_analysis_2025_clean = df_franchise_analysis_2025_clean[df_franchise_analysis_2025_clean['Filial_Lower'] != 'desconsiderar'].copy()
        
        # Remove Filiais vazias/nulas/string 'nan'
        df_franchise_analysis_2025_clean = df_franchise_analysis_2025_clean[
            (df_franchise_analysis_2025_clean['Filial_Lower'] != '') &
            (df_franchise_analysis_2025_clean['Filial_Lower'] != 'nan')
        ].copy()

        if not df_franchise_analysis_2025_clean.empty:
            reasons_summary_franchise_2025 = df_franchise_analysis_2025_clean.groupby('Filial').agg(
                Volume_2025=('Volume', 'sum')
            ).reset_index()
    
    # --- Data para análise de filial de 2024 ---
    df_franchise_analysis_2024_filtered = df_filtered[df_filtered['Ano Churn'] == 2024].copy()
    
    reasons_summary_franchise_2024 = pd.DataFrame()
    if 'Filial' in df_franchise_analysis_2024_filtered.columns and not df_franchise_analysis_2024_filtered['Filial'].isnull().all():
        df_franchise_analysis_2024_clean = df_franchise_analysis_2024_filtered.copy()
        df_franchise_analysis_2024_clean['Filial_Lower'] = df_franchise_analysis_2024_clean['Filial'].astype(str).str.strip().str.lower()
        # Se 'desconsiderar' puder ocorrer em Filial, adicione aqui:
        # df_franchise_analysis_2024_clean = df_franchise_analysis_2024_clean[df_franchise_analysis_2024_clean['Filial_Lower'] != 'desconsiderar'].copy()

        # Remove Filiais vazias/nulas/string 'nan'
        df_franchise_analysis_2024_clean = df_franchise_analysis_2024_clean[
            (df_franchise_analysis_2024_clean['Filial_Lower'] != '') &
            (df_franchise_analysis_2024_clean['Filial_Lower'] != 'nan')
        ].copy()

        if not df_franchise_analysis_2024_clean.empty:
            reasons_summary_franchise_2024 = df_franchise_analysis_2024_clean.groupby('Filial').agg(
                Volume_2024=('Volume', 'sum')
            ).reset_index()

    # --- Combinar e Calcular Variação para Filiais ---
    if not reasons_summary_franchise_2025.empty or not reasons_summary_franchise_2024.empty:
        df_combined_franchises = pd.merge(
            reasons_summary_franchise_2025,
            reasons_summary_franchise_2024,
            on='Filial',
            how='outer'
        ).fillna(0) # Preenche NaN volumes com 0 para cálculo

        # Calcular totais anuais para os percentuais (para Franquias)
        df_combined_franchises['Volume_2025_Total'] = df_combined_franchises['Volume_2025'].sum()
        df_combined_franchises['Volume_2024_Total'] = df_combined_franchises['Volume_2024'].sum()

        # Calcular Percentual 2025 (para Franquias)
        df_combined_franchises['Percentual_2025'] = df_combined_franchises.apply(
            lambda row: (row['Volume_2025'] / row['Volume_2025_Total']) * 100 if row['Volume_2025_Total'] > 0 else 0, axis=1
        )
        
        # Calcular Percentual 2024 (para Franquias)
        df_combined_franchises['Percentual_2024'] = df_combined_franchises.apply(
            lambda row: (row['Volume_2024'] / row['Volume_2024_Total']) * 100 if row['Volume_2024_Total'] > 0 else 0, axis=1
        )

        # Calcular Variação 2025 vs 2024 (RAZÃO, formatada como %) para Franquias
        df_combined_franchises['Variação 2025 vs 2024'] = df_combined_franchises.apply(
            lambda row: (
                ((row['Volume_2025'] / row['Volume_2024']) - 1) # Calcula a diferença percentual
                if row['Volume_2024'] > 0 else (
                    float('inf') if row['Volume_2025'] > 0 else 0 # Infinito se nova filial, 0 se ambas são 0
                )
            ),
            axis=1
        )
        
        # Renomear para exibição e formatar
        df_combined_franchises_display = df_combined_franchises.copy() # Criar uma cópia para renomear e formatar
        df_combined_franchises_display['Volume_2025'] = df_combined_franchises_display['Volume_2025'].astype(int)
        df_combined_franchises_display['Volume_2024'] = df_combined_franchises_display['Volume_2024'].astype(int)
        df_combined_franchises_display['Percentual_2025'] = df_combined_franchises_display['Percentual_2025'].map('{:.2f}%'.format)
        df_combined_franchises_display['Percentual_2024'] = df_combined_franchises_display['Percentual_2024'].map('{:.2f}%'.format)
        
        # Formato customizado para a coluna de variação (X.YY%) com vírgula decimal
        df_combined_franchises_display['Variação 2025 vs 2024'] = df_combined_franchises_display['Variação 2025 vs 2024'].apply(
            lambda x: f"{x:.2f}%".replace('.', ',') if pd.notna(x) and x != float('inf') else ("Nova Filial" if x == float('inf') else "0,00%")
        )
        
        # Renomear as colunas para o display final
        df_combined_franchises_display.rename(columns={
            'Filial': 'Filial',
            'Volume_2025': 'Volume 2025',
            'Percentual_2025': '% 2025',
            'Volume_2024': 'Volume 2024',
            'Percentual_2024': '% 2024'
        }, inplace=True)

        # Reordenar colunas para exibição (após renomear)
        df_combined_franchises_display = df_combined_franchises_display[[
            'Filial', 'Volume 2025', '% 2025',
            'Volume 2024', '% 2024', 'Variação 2025 vs 2024'
        ]]

        st.dataframe(df_combined_franchises_display, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado de Filial encontrado para 2024 ou 2025 com os filtros selecionados, ou todos os valores são vazios/nulos.")
    # --- FIM DA NOVA SEÇÃO ---


    st.markdown("---")
    st.markdown("Desenvolvido com Streamlit, Pandas e Plotly. Dados atualizados até a última execução do script.")

if __name__ == "__main__":
    main()
