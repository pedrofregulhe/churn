import pandas as pd
import os
import streamlit as st
import plotly.express as px
from datetime import datetime
import io


# --- 1. Configurações e Caminhos ---
data_dir = '.'
file_2024 = 'churn_2024.xlsx'
file_2025 = 'churn_2025.xlsx'
output_file_powerbi = 'dados_churn_consolidados_for_powerbi.xlsx'
output_path_powerbi = os.path.join(data_dir, output_file_powerbi)

file_active_base = 'base_ativa_clientes.xlsx'
file_backlog_churn = 'backlog_churn.xlsx'


# --- Função para Carregar e Transformar Dados de Churn e Base Ativa ---
@st.cache_data
def load_and_transform_data(data_folder, file_2024, file_2025, file_active_base, file_backlog_churn):
    """
    Carrega e combina os dados de churn de diferentes anos, a base ativa e o backlog,
    aplicando todas as transformações necessárias.
    """
    df_churn = pd.DataFrame()
    df_combined = pd.DataFrame()
    df_active_processed = pd.DataFrame()
    df_backlog_processed = pd.DataFrame()

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

    # --- INÍCIO: Carregamento e transformação dos dados de BACKLOG (NOVO) ---
    try:
        df_backlog_raw = pd.read_excel(os.path.join(data_dir, file_backlog_churn))

        month_col_map = {
            'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
            'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro' : 12,
            'Dez/24': 12 # NOVO: Mapeia 'Dez/24' para o mês 12
        }
        
        backlog_col_name = None
        for col in df_backlog_raw.columns:
            if 'Backlog' in str(col):
                backlog_col_name = col
                break
        if not backlog_col_name and 'Unnamed: 0' in df_backlog_raw.columns:
            backlog_col_name = 'Unnamed: 0'
        if not backlog_col_name and not df_backlog_raw.empty:
            backlog_col_name = df_backlog_raw.columns[0]
            
        if backlog_col_name:
            df_backlog_general = df_backlog_raw[df_backlog_raw[backlog_col_name] == 'Geral'].copy()
        else:
            st.warning("AVISO: Coluna de identificação para 'Geral' não encontrada no arquivo 'backlog_churn.xlsx'. Verifique o cabeçalho ou a estrutura.")
            df_backlog_processed = pd.DataFrame()
            return df_churn, df_active_processed, df_backlog_processed


        if not df_backlog_general.empty:
            df_backlog_melted = df_backlog_general.melt(
                id_vars=[backlog_col_name],
                var_name='Nome Mes Backlog',
                value_name='Volume Backlog'
            )
            
            df_backlog_melted = df_backlog_melted[df_backlog_melted['Nome Mes Backlog'].isin(month_col_map.keys())].copy()
            df_backlog_melted['Volume Backlog'] = pd.to_numeric(df_backlog_melted['Volume Backlog'], errors='coerce').fillna(0).astype(int)
            df_backlog_melted['Mes Backlog'] = df_backlog_melted['Nome Mes Backlog'].map(month_col_map)
            
            # --- NOVO: Lógica para atribuir o ano corretamente aos dados lidos da planilha ---
            df_backlog_melted['Ano Backlog'] = None
            for index, row in df_backlog_melted.iterrows():
                if row['Nome Mes Backlog'] == 'Dez/24': # Verifica se é a coluna específica de Dez/24
                    df_backlog_melted.loc[index, 'Ano Backlog'] = 2024
                else:
                    df_backlog_melted.loc[index, 'Ano Backlog'] = 2025 # Outros meses da planilha são 2025
            df_backlog_melted['Ano Backlog'] = df_backlog_melted['Ano Backlog'].astype(int)
            # --- FIM NOVO ---
            
            df_backlog_processed = df_backlog_melted[['Ano Backlog', 'Mes Backlog', 'Nome Mes Backlog', 'Volume Backlog']].copy()
            
            # --- NOVO: Adicionar manualmente o backlog de Dezembro de 2024 se não foi lido e não está presente ---
            # Soma os totais voluntário e involuntário + baixa ativo para Dez/24
            # (Se a planilha já tiver a coluna 'Dez/24' e a linha 'Geral', esta soma não será necessária,
            # mas estou sendo explícito com os valores que você forneceu caso a linha 'Geral' não pegue tudo).
            # O mais seguro é verificar se a linha 'Geral' para Dezembro de 2024 já existe.
            
            if not ((df_backlog_processed['Ano Backlog'] == 2024) & (df_backlog_processed['Mes Backlog'] == 12)).any():
                # Calcula o total Geral de Dezembro de 2024 a partir dos valores que você forneceu
                # Assumindo que esses valores são para a linha 'Geral' para Dezembro
                total_dez_2024_geral = 787 + 858 + 0 # Voluntário + Involuntário + Baixa de Ativo
                
                new_row_dec_2024 = pd.DataFrame([{
                    'Ano Backlog': 2024,
                    'Mes Backlog': 12,
                    'Nome Mes Backlog': 'Dezembro', # Nome do mês para exibição/lógica
                    'Volume Backlog': total_dez_2024_geral
                }])
                df_backlog_processed = pd.concat([df_backlog_processed, new_row_dec_2024], ignore_index=True)


        else:
            st.warning("AVISO: Linha 'Geral' não encontrada no arquivo 'backlog_churn.xlsx'. O KPI de Churn Operacional pode estar incorreto.")
            df_backlog_processed = pd.DataFrame()
            # Se a linha 'Geral' não for encontrada, mas ainda precisamos do Dezembro de 2024
            # Podemos adicionar o valor manual se df_backlog_processed estiver vazio após tentar ler
            if df_backlog_processed.empty:
                # Calcula o total Geral de Dezembro de 2024 a partir dos valores que você forneceu
                total_dez_2024_geral = 787 + 858 + 0
                new_row_dec_2024 = pd.DataFrame([{
                    'Ano Backlog': 2024,
                    'Mes Backlog': 12,
                    'Nome Mes Backlog': 'Dezembro',
                    'Volume Backlog': total_dez_2024_geral
                }])
                df_backlog_processed = pd.concat([df_backlog_processed, new_row_dec_2024], ignore_index=True)


    except FileNotFoundError as e:
        st.warning(f"AVISO: Arquivo .xlsx de BACKLOG não encontrado. O KPI de Churn Operacional não será exibido. Detalhes: {e}")
        df_backlog_processed = pd.DataFrame()
        # Adicionar o valor manual de Dezembro de 2024 mesmo se o arquivo não for encontrado
        total_dez_2024_geral = 787 + 858 + 0
        new_row_dec_2024 = pd.DataFrame([{
            'Ano Backlog': 2024,
            'Mes Backlog': 12,
            'Nome Mes Backlog': 'Dezembro',
            'Volume Backlog': total_dez_2024_geral
        }])
        df_backlog_processed = pd.concat([df_backlog_processed, new_row_dec_2024], ignore_index=True)
        
    except Exception as e:
        print(f"Erro detalhado no BACKLOG (Transformação): {e}")
        st.warning(f"AVISO: Problema ao carregar ou transformar dados de BACKLOG. O KPI de Churn Operacional pode estar incorreto. Detalhes: {e}")
        df_backlog_processed = pd.DataFrame()
        # Em caso de erro, ainda tenta adicionar o valor manual de Dezembro de 2024
        total_dez_2024_geral = 787 + 858 + 0
        new_row_dec_2024 = pd.DataFrame([{
            'Ano Backlog': 2024,
            'Mes Backlog': 12,
            'Nome Mes Backlog': 'Dezembro',
            'Volume Backlog': total_dez_2024_geral
        }])
        df_backlog_processed = pd.concat([df_backlog_processed, new_row_dec_2024], ignore_index=True)

    # --- Ajuste final: Remova duplicatas de Dezembro/2024 se houver ---
    df_backlog_processed.drop_duplicates(subset=['Ano Backlog', 'Mes Backlog'], inplace=True)
    # --- FIM NOVO ---


    # Continuação da transformação dos dados de CHURN
    df_combined.rename(columns={
        'Datacriacaoos': 'Data de Criacao da OS',
        'Statusos': 'Status da OS',
        'DATADESINSTALACAO': 'Data de Desinstalacao',
        'Formajuridica': 'Forma Juridica Original',
        'tipoChurn': 'Tipo de Churn',
        'Filialos': 'Filial'
    }, inplace=True)

    if 'Categoria4' in df_combined.columns:
        df_combined['Categoria4_Motivo'] = df_combined['Categoria4'].astype(str)
    else:
        df_combined['Categoria4_Motivo'] = None

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
        df_churn['Tipo de Cliente'] = df_churn['Forma Juridica Original'].apply(map_tipo_cliente)
        df_churn.dropna(subset=['Data de Desinstalacao'], inplace=True)
        df_churn['Ano Churn'] = df_churn['Data de Desinstalacao'].dt.year.astype(int)
        df_churn['Mes Churn'] = df_churn['Data de Desinstalacao'].dt.month.astype(int)

        month_names_map = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                           7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
        df_churn['Nome Mes Churn'] = df_churn['Mes Churn'].map(month_names_map)
        df_churn['Volume'] = 1
        df_churn['AnoMes'] = df_churn['Data de Desinstalacao'].dt.to_period('M').astype(str)

    for col in df_churn.select_dtypes(include=['object']).columns:
        df_churn[col] = df_churn[col].astype(str)

    for col in df_active_processed.select_dtypes(include=['object']).columns:
        df_active_processed[col] = df_active_processed[col].astype(str)
    
    return df_churn, df_active_processed, df_backlog_processed


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

    # --- Data da Última Atualização (apenas data) ---
    try:
        script_path = os.path.abspath(__file__)
        last_modified_timestamp = os.path.getmtime(script_path)
        last_modified_dt = datetime.fromtimestamp(last_modified_timestamp)

        portuguese_month_names = {
            1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril", 5: "maio", 6: "junho",
            7: "julho", 8: "agosto", 9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
        }
        
        formatted_date = f"{last_modified_dt.day} de {portuguese_month_names[last_modified_dt.month]} de {last_modified_dt.year}"
        
        st.markdown(f"**Última Atualização:** {formatted_date}")
    except Exception as e:
        st.warning(f"Não foi possível determinar a data da última atualização: {e}")
    # --- FIM: Data da Última Atualização ---

    df_churn, df_active_raw, df_backlog_raw = load_and_transform_data(data_dir, file_2024, file_2025, file_active_base, file_backlog_churn)

    st.sidebar.header("Filtros")

    if df_churn.empty or 'Ano Churn' not in df_churn.columns:
        st.error("ERRO: Dados de CHURN vazios ou incompletos. Verifique os arquivos de origem ou filtros.")
        st.stop()

    # --- Filtro de Anos com "Selecionar Todos" ---
    all_years = sorted(df_churn['Ano Churn'].unique())
    display_years = ["Todos"] + all_years
    selected_years_option = st.sidebar.multiselect(
        "Selecione o(s) Ano(s)",
        options=display_years,
        default=["Todos"]
    )
    if "Todos" in selected_years_option:
        selected_years = all_years
    else:
        selected_years = selected_years_option

    month_order_num_pt = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                          "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    month_abbr_order_pt = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                           "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    month_to_abbr_map = dict(zip(month_order_num_pt, month_abbr_order_pt))


    # --- Filtro de Meses com "Selecionar Todos" ---
    all_months = sorted(df_churn['Nome Mes Churn'].unique(), key=lambda x: month_order_num_pt.index(x))
    display_months = ["Todos"] + all_months
    selected_months_option = st.sidebar.multiselect(
        "Selecione o(s) Mês(es)",
        options=display_months,
        default=["Todos"]
    )
    if "Todos" in selected_months_option:
        selected_months = all_months
    else:
        selected_months = selected_months_option

    # --- Filtro de Tipo de Cliente com "Selecionar Todos" ---
    all_client_types = df_churn['Tipo de Cliente'].unique()
    display_client_types = ["Todos"] + list(all_client_types)
    selected_client_types_option = st.sidebar.multiselect(
        "Selecione o(s) Tipo(s) de Cliente",
        options=display_client_types,
        default=["Todos"]
    )
    if "Todos" in selected_client_types_option:
        selected_client_types = list(all_client_types)
    else:
        selected_client_types = selected_client_types_option


    if 'Tipo de Churn' in df_churn.columns and not df_churn['Tipo de Churn'].isnull().all():
        # --- Filtro de Tipo de Churn com "Selecionar Todos" ---
        all_churn_types = df_churn['Tipo de Churn'].unique()
        display_churn_types = ["Todos"] + list(all_churn_types)
        selected_churn_types_option = st.sidebar.multiselect(
            "Selecione o(s) Tipo(s) de Churn",
            options=display_churn_types,
            default=["Todos"]
        )
        if "Todos" in selected_churn_types_option:
            selected_churn_types = list(all_churn_types)
        else:
            selected_churn_types = selected_churn_types_option
    else:
        selected_churn_types = all_churn_types = []


    df_filtered = df_churn[
        (df_churn['Ano Churn'].isin(selected_years)) &
        (df_churn['Nome Mes Churn'].isin(selected_months)) &
        (df_churn['Tipo de Cliente'].isin(selected_client_types))
    ]
    if 'Tipo de Churn' in df_filtered.columns and selected_churn_types:
        df_filtered = df_filtered[df_filtered['Tipo de Churn'].isin(selected_churn_types)]


    if df_filtered.empty:
        st.warning("Nenhum dado de CHURN encontrado com os filtros selecionados. Ajuste os filtros na barra lateral.")
        st.stop()

    # --- INÍCIO DA SEÇÃO DE KPIS (Sem caixas, com alinhamento manual) ---
    st.header("Indicadores de Performance")

    # CSS para os estilos dos KPIs (ajuste conforme necessário para fontes e espaçamento)
    st.markdown("""
    <style>
    .kpi-container {
        text-align: center; /* Centraliza o texto dentro da coluna */
        padding: 10px 0; /* Espaçamento interno superior e inferior */
        margin-bottom: 15px; /* Espaço entre as linhas de KPIs */
        min-height: 80px; /* Altura mínima para tentar alinhar visualmente */
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .kpi-title {
        font-size: 1rem; /* Equivalente a h5 */
        color: #31333F;
        font-weight: bold;
        margin-bottom: 5px; /* Espaço entre o título e o valor */
    }
    .kpi-value {
        font-size: 1.8rem; /* Tamanho maior para o número principal */
        color: #000000; /* Preto mais forte para o valor */
        font-weight: bold;
        line-height: 1.2; /* Espaçamento entre linhas do valor */
    }
    .kpi-delta {
        font-size: 0.9rem; /* Tamanho para o delta/percentual */
        color: #6c757d; /* Cor cinza para o delta padrão */
        font-weight: normal;
    }
    .kpi-delta.positive { /* Usado para indicar um resultado 'bom' (ex: churn menor) */
        color: #28a745; /* Verde */
    }
    .kpi-delta.negative { /* Usado para indicar um resultado 'ruim' (ex: churn maior) */
        color: #dc3545; /* Vermelho */
    }
    </style>
    """, unsafe_allow_html=True)


    # Ajustado para 5 colunas agora
    col1, col_churn_operacional, col3_proj, col6_churn_rate, col4_base_ativa, col5_media_var = st.columns(6) # Mantive 6 colunas, col2 foi removida


    with col1:
        # KPI: Total de Churn Executado (2025) - Lógica Original Mantida
        df_churn_2025_kpi = df_churn[df_churn['Ano Churn'] == 2025].copy()
        df_churn_2025_kpi = df_churn_2025_kpi[
            (df_churn_2025_kpi['Nome Mes Churn'].isin(selected_months)) &
            (df_churn_2025_kpi['Tipo de Cliente'].isin(selected_client_types))
        ]
        if selected_churn_types:
            df_churn_2025_kpi = df_churn_2025_kpi[df_churn_2025_kpi['Tipo de Churn'].isin(selected_churn_types)]
        total_churn_2025_only = df_churn_2025_kpi['Volume'].sum()
        
        st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-title">Total de Churn Executado (2025)</div>
                <div class="kpi-value">{total_churn_2025_only:,.0f}</div>
            </div>
        """, unsafe_allow_html=True)

    # REMOVIDO: Antigo bloco do KPI "Variação Mensal (Absoluta)" (estava em col2)
    
        
    with col_churn_operacional: # Esta agora é a 2ª coluna visualmente
        # KPI: Total de Churn Operacional (2025) - Lógica Original Mantida
        churn_operacional_value = "N/A"
        churn_operacional_percentage = "N/A"

        if not df_backlog_raw.empty and selected_years and selected_months and not df_active_raw.empty:
            current_year_for_backlog = max(selected_years) if selected_years else 2025
            
            if len(selected_months) == 1:
                current_month_num = month_order_num_pt.index(selected_months[0]) + 1

                backlog_current_month_df = df_backlog_raw[
                    (df_backlog_raw['Ano Backlog'] == current_year_for_backlog) &
                    (df_backlog_raw['Mes Backlog'] == current_month_num)
                ]
                backlog_current_month = backlog_current_month_df['Volume Backlog'].sum()

                backlog_previous_month = 0
                # Verifica se o backlog de Dezembro/2024 está disponível para Janeiro/2025
                if current_month_num == 1 and current_year_for_backlog == 2025: # Se o mês é Janeiro e o ano é 2025
                    dez_2024_backlog_df = df_backlog_raw[
                        (df_backlog_raw['Ano Backlog'] == 2024) & 
                        (df_backlog_raw['Mes Backlog'] == 12)
                    ]
                    backlog_previous_month = dez_2024_backlog_df['Volume Backlog'].sum()
                elif current_month_num > 1: # Para outros meses (Fev em diante), pega o mês anterior do mesmo ano
                    backlog_previous_month_df = df_backlog_raw[
                        (df_backlog_raw['Ano Backlog'] == current_year_for_backlog) &
                        (df_backlog_raw['Mes Backlog'] == current_month_num - 1)
                    ]
                    backlog_previous_month = backlog_previous_month_df['Volume Backlog'].sum()


                delta_backlog = backlog_current_month - backlog_previous_month

                churn_volume_current_month = df_filtered[
                    (df_filtered['Ano Churn'] == current_year_for_backlog) &
                    (df_filtered['Mes Churn'] == current_month_num)
                ]['Volume'].sum()

                churn_operacional_value = delta_backlog + churn_volume_current_month

                active_base_current_month_df = df_active_raw[
                    (df_active_raw['Ano Base Ativa'] == current_year_for_backlog) &
                    (df_active_raw['Mes Base Ativa'] == current_month_num) &
                    (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
                ]
                active_base_current_month = active_base_current_month_df['Volume Base Ativa'].sum()

                if active_base_current_month > 0:
                    churn_operacional_percentage = (churn_operacional_value / active_base_current_month) * 100


            elif len(selected_months) > 1:
                total_churn_operacional = 0
                total_active_base_period = 0

                sorted_selected_month_nums = sorted([month_order_num_pt.index(m) + 1 for m in selected_months])

                for i, current_month_num in enumerate(sorted_selected_month_nums):
                    backlog_current_month_df = df_backlog_raw[
                        (df_backlog_raw['Ano Backlog'] == current_year_for_backlog) &
                        (df_backlog_raw['Mes Backlog'] == current_month_num)
                    ]
                    backlog_current_month = backlog_current_month_df['Volume Backlog'].sum()

                    backlog_previous_month = 0
                    if i > 0:
                        prev_month_num = sorted_selected_month_nums[i-1]
                        backlog_previous_month_df = df_backlog_raw[
                            (df_backlog_raw['Ano Backlog'] == current_year_for_backlog) &
                            (df_backlog_raw['Mes Backlog'] == prev_month_num)
                        ]
                        backlog_previous_month = backlog_previous_month_df['Volume Backlog'].sum()
                    elif current_month_num == 1 and current_year_for_backlog > (df_backlog_raw['Ano Backlog'].min() if not df_backlog_raw.empty else current_year_for_backlog):
                        backlog_previous_month_df = df_backlog_raw[
                            (df_backlog_raw['Ano Backlog'] == current_year_for_backlog - 1) &
                            (df_backlog_raw['Mes Backlog'] == 12)
                        ]
                        backlog_previous_month = backlog_previous_month_df['Volume Backlog'].sum()
                    
                    delta_backlog = backlog_current_month - backlog_previous_month

                    churn_volume_current_month = df_filtered[
                        (df_filtered['Ano Churn'] == current_year_for_backlog) &
                        (df_filtered['Mes Churn'] == current_month_num)
                    ]['Volume'].sum()
                    
                    total_churn_operacional += (delta_backlog + churn_volume_current_month)

                    active_base_current_month_df = df_active_raw[
                        (df_active_raw['Ano Base Ativa'] == current_year_for_backlog) &
                        (df_active_raw['Mes Base Ativa'] == current_month_num) &
                        (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
                    ]
                    total_active_base_period += active_base_current_month_df['Volume Base Ativa'].sum()
                
                churn_operacional_value = total_churn_operacional

                if total_active_base_period > 0:
                    churn_operacional_percentage = (churn_operacional_value / total_active_base_period) * 100
                else:
                    churn_operacional_percentage = "N/A"
        
        display_value_co = f"{int(churn_operacional_value):,.0f}".replace(",", ".") if isinstance(churn_operacional_value, (int, float)) else str(churn_operacional_value)
        
        delta_text_co = ""
        if isinstance(churn_operacional_percentage, (int, float)):
            delta_text_co = f'<div class="kpi-delta">({churn_operacional_percentage:.2f}%)</div>'.replace('.', ',')
        else:
            delta_text_co = f'<div class="kpi-delta">({str(churn_operacional_percentage)})</div>'

        st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-title">Total de Churn Operacional (2025)</div>
                <div class="kpi-value">{display_value_co}</div>
                {delta_text_co}
            </div>
        """, unsafe_allow_html=True)


    with col3_proj: # Esta agora é a 3ª coluna visualmente
        # KPI: Projeção Anual Churn - Lógica Original Mantida
        projected_annual_churn = 0 
        if selected_years:
            current_year_churn_proj = max(selected_years)
            df_current_year_churn = df_filtered[df_filtered['Ano Churn'] == current_year_churn_proj]

            if not df_current_year_churn.empty:
                max_month_data_churn = df_current_year_churn['Mes Churn'].max()
                churn_accumulated = df_current_year_churn[df_current_year_churn['Mes Churn'] <= max_month_data_churn]['Volume'].sum()
                num_months_data_churn = df_current_year_churn['Mes Churn'].nunique()

                if num_months_data_churn > 0:
                    projected_annual_churn = (churn_accumulated / num_months_data_churn) * 12
        
        display_value_proj = f"{int(projected_annual_churn):,.0f}".replace(",", ".") if projected_annual_churn > 0 else "N/A"
        help_text_proj = "Os dados neste KPI referem-se ao ano selecionado para projeção." if projected_annual_churn > 0 else "Sem dados para projeção."
        
        st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-title">Projeção Anual Churn ({max(selected_years) if selected_years else 'N/A'})</div>
                <div class="kpi-value">{display_value_proj}</div>
            </div>
        """, unsafe_allow_html=True, help=help_text_proj)


    with col6_churn_rate: # Esta agora é a 4ª coluna visualmente
        # KPI: Projeção Churn Rate Anual (%) - Lógica Original Mantida
        churn_rate_value = "N/A"
        
        projected_annual_churn_calc = 0
        avg_monthly_active_calc = 0

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

        if not df_active_raw.empty:
            current_year_active_proj_calc = 2025 
            df_current_year_active_filtered_calc = df_active_raw[
                (df_active_raw['Mes Base Ativa'].isin([month_order_num_pt.index(m)+1 for m in selected_months])) &
                (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
            ].copy()
            if selected_client_types:
                df_current_year_active_filtered_calc = df_current_year_active_filtered_calc[df_current_year_active_filtered_calc['Tipo de Cliente Base Ativa'].isin(selected_client_types)]
            
            if not df_current_year_active_filtered_calc.empty:
                total_active_until_now_calc = df_current_year_active_filtered_calc['Volume Base Ativa'].sum()
                num_months_active_data_calc = df_current_year_active_filtered_calc['Mes Base Ativa'].nunique()
                if num_months_active_data_calc > 0:
                    avg_monthly_active_calc = total_active_until_now_calc / num_months_active_data_calc

        if projected_annual_churn_calc is not None and avg_monthly_active_calc is not None and avg_monthly_active_calc > 0:
            churn_rate_value = (projected_annual_churn_calc / avg_monthly_active_calc) * 100 
        
        display_value_cr = f"{churn_rate_value:.2f}%".replace('.', ',') if isinstance(churn_rate_value, (int, float)) else "N/A"
        
        st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-title">Projeção Churn Rate Anual (%)</div>
                <div class="kpi-value">{display_value_cr}</div>
            </div>
        """, unsafe_allow_html=True)


    with col4_base_ativa: # Esta agora é a 5ª coluna visualmente
        # KPI: Média Mensal Base Ativa - Lógica Original Mantida
        if avg_monthly_active_calc > 0:
            display_value_b = f"{int(avg_monthly_active_calc):,.0f}".replace(",", ".")
            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-title">Média Mensal Base Ativa</div>
                    <div class="kpi-value">{display_value_b}</div>
                </div>
            """, unsafe_allow_html=True, help="Os dados neste KPI referem-se ao ano de 2025.")
        else:
            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-title">Média Mensal Base Ativa</div>
                    <div class="kpi-value">N/A</div>
                </div>
            """, unsafe_allow_html=True, help="Arquivo de base ativa não carregado ou vazio.")

    with col5_media_var: # Esta agora é a 6ª coluna visualmente
        # KPI: Variação de Churn Ex. 2025 vs 2024 - Lógica Ajustada para Absoluto + Porcentagem
        df_churn_for_kpi_comparison = df_churn.copy()
        if selected_client_types:
            df_churn_for_kpi_comparison = df_churn_for_kpi_comparison[df_churn_for_kpi_comparison['Tipo de Cliente'].isin(selected_client_types)]
        if selected_churn_types:
            df_churn_for_kpi_comparison = df_churn_for_kpi_comparison[df_churn_for_kpi_comparison['Tipo de Churn'].isin(selected_churn_types)]

        # Lógica para obter a variação anual (média das variações mensais)
        df_monthly_volumes_kpi = df_churn_for_kpi_comparison[
            df_churn_for_kpi_comparison['Ano Churn'].isin([2024, 2025])
        ].groupby(['Ano Churn', 'Mes Churn']).agg(
                Volume_Churn=('Volume', 'sum')
        ).reset_index()

        df_comparison = df_monthly_volumes_kpi.pivot_table(
            index='Mes Churn',
            columns='Ano Churn',
            values='Volume_Churn'
        ).reset_index()

        selected_month_nums = [month_order_num_pt.index(m)+1 for m in selected_months]
        df_comparison_filtered_months = df_comparison[df_comparison['Mes Churn'].isin(selected_month_nums)].copy()

        df_comparison_filtered_months[2024] = df_comparison_filtered_months.get(2024, pd.Series(0, index=df_comparison_filtered_months.index)).fillna(0)
        df_comparison_filtered_months[2025] = df_comparison_filtered_months.get(2025, pd.Series(0, index=df_comparison_filtered_months.index)).fillna(0)

        # Calcular a diferença absoluta total para os meses selecionados
        absolute_diff_yoy = df_comparison_filtered_months[2025].sum() - df_comparison_filtered_months[2024].sum()

        # Calcular a variação percentual média mensal
        df_comparison_filtered_months['Monthly_Variation'] = pd.NA
        valid_comparison_rows = df_comparison_filtered_months[df_comparison_filtered_months[2024] > 0]
        if not valid_comparison_rows.empty:
            df_comparison_filtered_months.loc[valid_comparison_rows.index, 'Monthly_Variation'] = \
                (valid_comparison_rows[2025] - valid_comparison_rows[2024]) / valid_comparison_rows[2024]
        average_monthly_percentage_variation = df_comparison_filtered_months['Monthly_Variation'].mean()

        # Formatação para exibição
        display_value_yoy = f"{int(absolute_diff_yoy):,.0f}".replace(",", ".") if isinstance(absolute_diff_yoy, (int, float)) else "N/A"
        
        percentage_text_yoy = ""
        if pd.notna(average_monthly_percentage_variation):
            percentage_text_yoy = f'<div class="kpi-delta">({average_monthly_percentage_variation:.2%})</div>'.replace('.', ',')
        else:
            percentage_text_yoy = '<div class="kpi-delta">(N/A)</div>'

        delta_color_class_yoy = ""
        if isinstance(absolute_diff_yoy, (int, float)):
            delta_color_class_yoy = "positive" if absolute_diff_yoy < 0 else "negative" # Churn menor (negativo) é positivo (verde)

        st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-title">Variação de Churn Ex. 2025 vs 2024</div>
                <div class="kpi-value {delta_color_class_yoy}">{display_value_yoy}</div>
                {percentage_text_yoy}
            </div>
        """, unsafe_allow_html=True, help="Variação absoluta do churn (2025 vs 2024) para os meses selecionados. A porcentagem é a média das variações percentuais mensais.")
    # --- FIM DA SEÇÃO DE KPIS ---
    st.markdown("---") # Separador após a seção de KPIs

    # --- Abas para organizar o conteúdo ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Volume Mensal de Churn", "Churn por Tipo de Cliente", "Volume de Churn Mensal por Tipo", "Motivos de Cancelamento", "Churn por Filial"])

    with tab1:
        st.header("Churn Mensal por Ano e Variação")
        df_active_monthly_volumes = pd.DataFrame()
        if not df_active_raw.empty:
            df_active_filtered_for_chart = df_active_raw[
                (df_active_raw['Mes Base Ativa'].isin([month_order_num_pt.index(m)+1 for m in selected_months])) &
                (df_active_raw['Tipo de Cliente Base Ativa'].isin(selected_client_types))
            ].copy()
            
            df_active_monthly_volumes = df_active_filtered_for_chart.groupby(['Ano Base Ativa', 'Mes Base Ativa', 'Nome Mes Ativa']).agg(
                Volume_Base_Ativa=('Volume Base Ativa', 'sum')
            ).reset_index()
            
            df_active_monthly_volumes.rename(columns={
                'Ano Base Ativa': 'Ano Churn',
                'Mes Base Ativa': 'Mes Churn',
                'Nome Mes Ativa': 'Nome Mes Churn'
            }, inplace=True)

        df_plot_monthly_volume = df_filtered.groupby(['Ano Churn', 'Mes Churn', 'Nome Mes Churn']).agg(
            Volume_Churn=('Volume', 'sum')
        ).reset_index().sort_values(by=['Ano Churn', 'Mes Churn'])

        df_plot_monthly_volume = pd.merge(
            df_plot_monthly_volume,
            df_active_monthly_volumes,
            on=['Ano Churn', 'Mes Churn', 'Nome Mes Churn'],
            how='left'
        )
        
        df_plot_monthly_volume['Churn_Rate'] = df_plot_monthly_volume.apply(
            lambda row: (row['Volume_Churn'] / row['Volume_Base_Ativa'] * 100) if row['Volume_Base_Ativa'] > 0 else float('nan'),
            axis=1
        )
        
        df_plot_monthly_volume['Bar_Text_Label'] = df_plot_monthly_volume.apply(
            lambda row: (
                f"{row['Volume_Churn']:,.0f}".replace(",", ".") +
                (f"<br>{row['Churn_Rate']:.2f}%" if pd.notna(row['Churn_Rate']) and row['Ano Churn'] == 2025 else "")
            ),
            axis=1
        )

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

        fig_monthly_bar_with_variation = px.bar(
            df_plot_monthly,
            x="X_Axis_Month_Label",
            y="Volume_Churn",
            color=df_plot_monthly['Ano Churn'].astype(str),
            barmode="group",
            labels={
                "X_Axis_Month_Label": "Mês (Variação YoY)",
                "color": "Ano"
            },
            category_orders={"X_Axis_Month_Label": sorted(df_yoy_pivot_for_label['X_Axis_Month_Label'].unique(), key=lambda x: month_order_num_pt.index(x.split('<br>')[0]) if '<br>' in x else month_order_num_pt.index(x))},
            text='Bar_Text_Label'
        )

        fig_monthly_bar_with_variation.update_traces(
            textposition='outside',
            textfont=dict(color='black', weight='bold', size=10),
            textangle=0
        )

        fig_monthly_bar_with_variation.update_traces(
            hovertemplate="<b>Mês:</b> %{customdata[1]}<br><b>Ano:</b> %{fullData.name}<br><b>Volume:</b> %{y:,.0f}" +
                          "<br><b>Variação (25 vs 24):</b> %{customdata[0]:.1%}<extra></extra>" +
                          "<br><b>Informação na barra:</b> %{text}<extra></extra>",
            customdata=df_plot_monthly[['YoY_Variation', 'Nome Mes Churn']]
        )

        fig_monthly_bar_with_variation.update_layout(
            hovermode="x unified",
            yaxis_title="",
            legend=dict(
                font=dict(
                    size=12,
                    color="black",
                    family="Arial",
                    weight="bold"
                ),
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="right",
                x=1.1
            ),
            xaxis_title="Mês (Variação YoY)",
            xaxis=dict(tickangle=0)
        )
        st.plotly_chart(fig_monthly_bar_with_variation, use_container_width=True)

    with tab2:
        st.header("Distribuição de Churn por Tipo de Cliente")

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


        df_plot_client_type_2024 = df_churn_2024.groupby('Tipo de Cliente').agg(
            Volume_Churn=('Volume', 'sum')
        ).reset_index().sort_values(by='Volume_Churn', ascending=False)

        df_plot_client_type_2025 = df_churn_2025.groupby('Tipo de Cliente').agg(
            Volume_Churn=('Volume', 'sum')
        ).reset_index().sort_values(by='Volume_Churn', ascending=False)

        col_2024, col_2025, col_comparison = st.columns([1, 1, 1]) 

        with col_2024:
            st.markdown("<h3 style='text-align: center;'>Consolidado 2024</h3>", unsafe_allow_html=True)
            if not df_plot_client_type_2024.empty:
                fig_client_type_2024 = px.pie(
                    df_plot_client_type_2024,
                    values="Volume_Churn",
                    names="Tipo de Cliente",
                    hole=0.4
                )
                fig_client_type_2024.update_traces(textinfo="percent+label", pull=[0.05]*len(df_plot_client_type_2024))
                fig_client_type_2024.update_layout(
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.2,
                        xanchor="center",
                        x=0.5,
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
            st.markdown("<h3 style='text-align: center;'>Consolidado 2025</h3>", unsafe_allow_html=True)
            if not df_plot_client_type_2025.empty:
                fig_client_type_2025 = px.pie(
                    df_plot_client_type_2025,
                    values="Volume_Churn",
                    names="Tipo de Cliente",
                    hole=0.4
                )
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
            ).fillna(0)

            df_comparison_client_type['Diferenca_Absoluta'] = df_comparison_client_type['Volume_2025'] - df_comparison_client_type['Volume_2024']
            
            df_comparison_client_type['Diferenca_Percentual'] = df_comparison_client_type.apply(
                lambda row: ((row['Volume_2025'] - row['Volume_2024']) / row['Volume_2024']) * 100 if row['Volume_2024'] != 0 else (100 if row['Volume_2025'] > 0 else 0),
                axis=1
            )
            
            if not df_comparison_client_type.empty:
                with st.container(border=True):
                    st.markdown("<h4 style='text-align: center; color: gray;'>Diferenças por Tipo de Cliente</h4>", unsafe_allow_html=True)
                    
                    for index, row in df_comparison_client_type.iterrows():
                        tipo_cliente = row['Tipo de Cliente']
                        diff_abs = row['Diferenca_Absoluta']
                        diff_perc = row['Diferenca_Percentual']
                                            
                        delta_color = "normal" if diff_abs < 0 else "inverse" 

                        col_left_spacer_inner, col_metric_content_inner, col_right_spacer_inner = st.columns([0.2, 0.6, 0.2])
                        
                        with col_metric_content_inner:
                            st.metric(
                                label=f"**{tipo_cliente}**",
                                value=f"{int(diff_abs):,.0f}".replace(",", "."),
                                delta=f"{diff_perc:.1f}%",
                                delta_color=delta_color
                            )
            else:
                st.info("Nenhuma variação para exibir com os filtros selecionado.")
        
    with tab3: # Nova aba para "Volume de Churn Mensal por Tipo"
        st.header("Volume de Churn Mensal por Tipo")
        
        if 'Tipo de Churn' in df_filtered.columns and not df_filtered['Tipo de Churn'].isnull().all():
            df_plot_churn_type_monthly = df_filtered.groupby(['Ano Churn', 'Mes Churn', 'Nome Mes Churn', 'Tipo de Churn']).agg(
                Volume_Churn=('Volume', 'sum')
            ).reset_index()

            df_plot_churn_type_monthly['Nome Mes Abreviado'] = df_plot_churn_type_monthly['Nome Mes Churn'].map(month_to_abbr_map)

            ordered_abbr_months = month_abbr_order_pt
            
            fig_churn_type_monthly_stacked = px.bar(
                df_plot_churn_type_monthly,
                x="Nome Mes Abreviado",
                y="Volume_Churn",
                color="Tipo de Churn",
                facet_col="Ano Churn",
                barmode="stack",
                labels={
                    "Nome Mes Abreviado": "Mês",
                    "Volume_Churn": "Volume de Churn",
                    "Ano Churn": "Ano",
                    "Tipo de Churn": "Tipo de Churn"
                },
                category_orders={"Nome Mes Abreviado": ordered_abbr_months},
                text_auto=True
            )
            fig_churn_type_monthly_stacked.update_traces(
                textposition='inside',
                textfont=dict(color='white', weight='bold', size=12),
                textangle=0
            )
            fig_churn_type_monthly_stacked.update_layout(
                title="",
                xaxis_title="Mês",
                yaxis_title="Volume de Churn",
                legend=dict(
                    font=dict(
                        size=12,
                        color="black",
                        family="Arial",
                        weight="bold"
                    ),
                    title_text="",
                    orientation="h",
                    yanchor="bottom",
                    y=-0.25,
                    xanchor="center",
                    x=0.5
                ),
                hovermode="x unified"
            )
            fig_churn_type_monthly_stacked.for_each_annotation(lambda a: a.update(text=a.text.replace("Ano Churn=", "")))

            st.plotly_chart(fig_churn_type_monthly_stacked, use_container_width=True)
        else:
            st.warning("Não há dados de 'Tipo de Churn' para exibir o gráfico mensal empilhado.")

    with tab4:
        st.header("Análise de Motivos de Cancelamento por Ano")

        df_cancellation_analysis_2025_filtered = df_filtered[df_filtered['Ano Churn'] == 2025].copy()
        
        reasons_summary_2025 = pd.DataFrame()
        if 'Categoria4_Motivo' in df_cancellation_analysis_2025_filtered.columns and not df_cancellation_analysis_2025_filtered['Categoria4_Motivo'].isnull().all():
            df_cancellation_analysis_2025_clean = df_cancellation_analysis_2025_filtered.copy()
            df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] = df_cancellation_analysis_2025_clean['Categoria4_Motivo'].astype(str).str.strip().str.lower()
            df_cancellation_analysis_2025_clean = df_cancellation_analysis_2025_clean[
                (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != '') &
                (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != 'nan') &
                (df_cancellation_analysis_2025_clean['Categoria4_Motivo_Lower'] != 'desconsiderar')
            ].copy()

            if not df_cancellation_analysis_2025_clean.empty:
                reasons_summary_2025 = df_cancellation_analysis_2025_clean.groupby('Categoria4_Motivo').agg(
                    Volume_2025=('Volume', 'sum')
                ).reset_index()
        
        df_cancellation_analysis_2024_filtered = df_filtered[df_filtered['Ano Churn'] == 2024].copy()
        
        reasons_summary_2024 = pd.DataFrame()
        if 'Categoria4_Motivo' in df_cancellation_analysis_2024_filtered.columns and not df_cancellation_analysis_2024_filtered['Categoria4_Motivo'].isnull().all():
            df_cancellation_analysis_2024_clean = df_cancellation_analysis_2024_filtered.copy()
            df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] = df_cancellation_analysis_2024_clean['Categoria4_Motivo'].astype(str).str.strip().str.lower()
            df_cancellation_analysis_2024_clean = df_cancellation_analysis_2024_clean[
                (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != '') &
                (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != 'nan') &
                (df_cancellation_analysis_2024_clean['Categoria4_Motivo_Lower'] != 'desconsiderar')
            ].copy()

            if not df_cancellation_analysis_2024_clean.empty:
                reasons_summary_2024 = df_cancellation_analysis_2024_clean.groupby('Categoria4_Motivo').agg(
                    Volume_2024=('Volume', 'sum')
                ).reset_index()

        if not reasons_summary_2025.empty or not reasons_summary_2024.empty:
            df_combined_reasons = pd.merge(
                reasons_summary_2025,
                reasons_summary_2024,
                on='Categoria4_Motivo',
                how='outer'
            ).fillna(0)

            df_combined_reasons['Volume_2025_Total'] = df_combined_reasons['Volume_2025'].sum()
            df_combined_reasons['Volume_2024_Total'] = df_combined_reasons['Volume_2024'].sum()

            df_combined_reasons['Percentual_2025'] = df_combined_reasons.apply(
                lambda row: (row['Volume_2025'] / row['Volume_2025_Total']) * 100 if row['Volume_2025_Total'] > 0 else 0, axis=1
            )
            
            df_combined_reasons['Percentual_2024'] = df_combined_reasons.apply(
                lambda row: (row['Volume_2024'] / row['Volume_2024_Total']) * 100 if row['Volume_2024_Total'] > 0 else 0, axis=1
            )

            df_combined_reasons['Variação 2025 vs 2024'] = df_combined_reasons.apply(
                lambda row: (
                    ((row['Volume_2025'] / row['Volume_2024']) - 1)
                    if row['Volume_2024'] > 0 else (
                        float('inf') if row['Volume_2025'] > 0 else 0
                    )
                ),
                axis=1
            )
            
            df_combined_reasons_display = df_combined_reasons.copy()
            df_combined_reasons_display['Volume_2025'] = df_combined_reasons_display['Volume_2025'].astype(int)
            df_combined_reasons_display['Volume_2024'] = df_combined_reasons_display['Volume_2024'].astype(int)
            df_combined_reasons_display['Percentual_2025'] = df_combined_reasons_display['Percentual_2025'].map('{:.2f}%'.format)
            df_combined_reasons_display['Percentual_2024'] = df_combined_reasons_display['Percentual_2024'].map('{:.2f}%'.format)
            
            df_combined_reasons_display['Variação 2025 vs 2024'] = df_combined_reasons_display['Variação 2025 vs 2024'].apply(
                lambda x: f"{x:.2f}%".replace('.', ',') if pd.notna(x) and x != float('inf') else ("Novo Motivo" if x == float('inf') else "0,00%")
            )
            
            df_combined_reasons_display.rename(columns={
                'Categoria4_Motivo': 'Motivo de Cancelamento',
                'Volume_2025': 'Volume 2025',
                'Percentual_2025': '% 2025',
                'Volume_2024': 'Volume 2024',
                'Percentual_2024': '% 2024'
            }, inplace=True)

            df_combined_reasons_display = df_combined_reasons_display[[
                'Motivo de Cancelamento', 'Volume 2025', '% 2025',
                'Volume 2024', '% 2024', 'Variação 2025 vs 2024'
            ]]

            st.dataframe(df_combined_reasons_display, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum dado de motivos de cancelamento (da Categoria4) encontrado para 2024 ou 2025 com os filtros selecionados, ou todos foram 'Desconsiderar' / vazios.")

    with tab5:
        st.header("Análise de Churn por Filial por Ano")

        df_franchise_analysis_2025_filtered = df_filtered[df_filtered['Ano Churn'] == 2025].copy()
        
        reasons_summary_franchise_2025 = pd.DataFrame()
        if 'Filial' in df_franchise_analysis_2025_filtered.columns and not df_franchise_analysis_2025_filtered['Filial'].isnull().all():
            df_franchise_analysis_2025_clean = df_franchise_analysis_2025_filtered.copy()
            df_franchise_analysis_2025_clean['Filial_Lower'] = df_franchise_analysis_2025_clean['Filial'].astype(str).str.strip().str.lower()
            
            df_franchise_analysis_2025_clean = df_franchise_analysis_2025_clean[
                (df_franchise_analysis_2025_clean['Filial_Lower'] != '') &
                (df_franchise_analysis_2025_clean['Filial_Lower'] != 'nan')
            ].copy()

            if not df_franchise_analysis_2025_clean.empty:
                reasons_summary_franchise_2025 = df_franchise_analysis_2025_clean.groupby('Filial').agg(
                    Volume_2025=('Volume', 'sum')
                ).reset_index()
        
        df_franchise_analysis_2024_filtered = df_filtered[df_filtered['Ano Churn'] == 2024].copy()
        
        reasons_summary_franchise_2024 = pd.DataFrame()
        if 'Filial' in df_franchise_analysis_2024_filtered.columns and not df_franchise_analysis_2024_filtered['Filial'].isnull().all():
            df_franchise_analysis_2024_clean = df_franchise_analysis_2024_filtered.copy()
            df_franchise_analysis_2024_clean['Filial_Lower'] = df_franchise_analysis_2024_clean['Filial'].astype(str).str.strip().str.lower()

            df_franchise_analysis_2024_clean = df_franchise_analysis_2024_clean[
                (df_franchise_analysis_2024_clean['Filial_Lower'] != '') &
                (df_franchise_analysis_2024_clean['Filial_Lower'] != 'nan')
            ].copy()

            if not df_franchise_analysis_2024_clean.empty:
                reasons_summary_franchise_2024 = df_franchise_analysis_2024_clean.groupby('Filial').agg(
                    Volume_2024=('Volume', 'sum')
                ).reset_index()

        if not reasons_summary_franchise_2025.empty or not reasons_summary_franchise_2024.empty:
            df_combined_franchises = pd.merge(
                reasons_summary_franchise_2025,
                reasons_summary_franchise_2024,
                on='Filial',
                how='outer'
            ).fillna(0)

            df_combined_franchises['Volume_2025_Total'] = df_combined_franchises['Volume_2025'].sum()
            df_combined_franchises['Volume_2024_Total'] = df_combined_franchises['Volume_2024'].sum()

            df_combined_franchises['Percentual_2025'] = df_combined_franchises.apply(
                lambda row: (row['Volume_2025'] / row['Volume_2025_Total']) * 100 if row['Volume_2025_Total'] > 0 else 0, axis=1
            )
            
            df_combined_franchises['Percentual_2024'] = df_combined_franchises.apply(
                lambda row: (row['Volume_2024'] / row['Volume_2024_Total']) * 100 if row['Volume_2024_Total'] > 0 else 0, axis=1
            )

            df_combined_franchises['Variação 2025 vs 2024'] = df_combined_franchises.apply(
                lambda row: (
                    ((row['Volume_2025'] / row['Volume_2024']) - 1)
                    if row['Volume_2024'] > 0 else (
                        float('inf') if row['Volume_2025'] > 0 else 0
                    )
                ),
                axis=1
            )
            
            df_combined_franchises_display = df_combined_franchises.copy()
            df_combined_franchises_display['Volume_2025'] = df_combined_franchises_display['Volume_2025'].astype(int)
            df_combined_franchises_display['Volume_2024'] = df_combined_franchises_display['Volume_2024'].astype(int)
            df_combined_franchises_display['Percentual_2025'] = df_combined_franchises_display['Percentual_2025'].map('{:.2f}%'.format)
            df_combined_franchises_display['Percentual_2024'] = df_combined_franchises_display['Percentual_2024'].map('{:.2f}%'.format)
            
            df_combined_franchises_display['Variação 2025 vs 2024'] = df_combined_franchises_display['Variação 2025 vs 2024'].apply(
                lambda x: f"{x:.2f}%".replace('.', ',') if pd.notna(x) and x != float('inf') else ("Nova Filial" if x == float('inf') else "0,00%")
            )
            
            df_combined_franchises_display.rename(columns={
                'Filial': 'Filial',
                'Volume_2025': 'Volume 2025',
                'Percentual_2025': '% 2025',
                'Volume_2024': 'Volume 2024',
                'Percentual_2024': '% 2024'
            }, inplace=True)

            df_combined_franchises_display = df_combined_franchises_display[[
                'Filial', 'Volume 2025', '% 2025',
                'Volume 2024', '% 2024', 'Variação 2025 vs 2024'
            ]]

            st.dataframe(df_combined_franchises_display, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum dado de Filial encontrado para 2024 ou 2025 com os filtros selecionado.")

    st.markdown("---")
    st.markdown("Desenvolvido com Streamlit, Pandas e Plotly. Dados atualizados até a última execução do script.")

if __name__ == "__main__":
    main()