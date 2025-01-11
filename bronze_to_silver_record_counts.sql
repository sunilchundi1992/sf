create or replace procedure bronze_silver_counts(database_name varchar)
returns variant
language python 
handler = 'run'
runtime_version = '3.11'
packages =  ('snowflake-snowpark-python', 'pandas')
as
$$
from snowflake.snowpark import Session as session
from datetime import datetime
import json

current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def GENERATE_HTML_TABLE(JSON_DATA):
    # Define column widths for each header
    column_widths = ["120px", "320px", "320px", "120px", "120px", "160px"]
    
    # Parse the JSON input into Python objects
    DATA = json.loads(JSON_DATA)
    
    # Start the HTML structure with a Snowflake logo and table headers
    HTML = f"""
        <img src="https://s26.q4cdn.com/463892824/files/doc_multimedia/HI_RES-_Snowflake_Logo_Blue_1800x550.jpg" alt="Snowflake logo" height="72">
        <p><strong>Bronze to Silver tables comprision mistaches details</strong>
        <br>Log in to Snowsight to see more details.</p>
        <table border="1" style="border-color:#DEE3EA" cellpadding="5" cellspacing="0">
            <thead>
                <tr>
    """
    
    # Update headers with your new fields
    headers = ["Comparison ID", "Bronze Table Name", "Silver Table Name", "Bronze Count", "Silver Count", "Status"]
    
    # Generate the header row of the HTML table
    for i, header in enumerate(headers):
        HTML += f'<th scope="col" style="text-align:left; width: {column_widths[i]}">{header}</th>'
    
    HTML += """        
        </tr>
    </thead>
    <tbody>
    """
    
    # Loop through each row of the JSON data and generate table rows
    for ROW_DATA in DATA:
        HTML += "<tr>"
        for header in headers:
            # Create the key by converting the header to uppercase and replacing spaces with underscores
            key = header.replace(" ", "_").upper()
            # Retrieve the data from the row, or default to an empty string if the key is not found
            CELL_DATA = ROW_DATA.get(key, "")
            # Add the data as a table cell
            HTML += f'<td style="text-align:left; width: {column_widths[headers.index(header)]}">{CELL_DATA}</td>'
        HTML += "</tr>"
    
    # Close the table and HTML structure
    HTML += """
  </tbody>
  </table>
  """
  
    # Return the generated HTML
    return HTML

def run(session, database_name):
    unique_id_row = session.sql(f"SELECT {database_name}.METADATA.COUNT_COMPARISON_RESULTS_SEQ.NEXTVAL AS UNIQUE_ID").collect()
    unique_id = unique_id_row[0][0]
    dataframe = session.sql(f""" 
                               INSERT INTO {database_name}.METADATA.COUNT_COMPARISON_RESULTS(
                                COMPARISON_ID,
                                bronze_table_name, 
                                silver_table_name, 
                                bronze_count, 
                                silver_count, 
                                status
                            )
                            SELECT
                                {unique_id},
                                m.bronze_table_name, 
                                m.silver_table_name, 
                                COALESCE(bronze_count, 0) AS bronze_count, 
                                COALESCE(silver_count, 0) AS silver_count,
                                CASE
                                    WHEN COALESCE(bronze_count, 0) = COALESCE(silver_count, 0) THEN 'MATCH'
                                    ELSE 'MISMATCH'
                                END AS status
                            FROM  
                            (
                                SELECT 
                                    m.bronze_table_name,
                                    m.silver_table_name,
                                    bronze.row_count AS bronze_count,
                                    silver.row_count AS silver_count
                                FROM 
                                    {database_name}.metadata.table_name_mapping m
                                LEFT JOIN (
                                    SELECT table_name, row_count
                                    FROM {database_name}.information_schema.tables
                                    WHERE table_schema = 'BRONZE' AND table_type = 'BASE TABLE'
                                ) bronze
                                ON m.bronze_table_name = bronze.table_name
                                LEFT JOIN (
                                    SELECT table_name, row_count
                                    FROM {database_name}.information_schema.tables
                                    WHERE table_schema = 'SILVER' AND table_type = 'BASE TABLE'
                                ) silver
                                ON m.silver_table_name = silver.table_name
                            ) AS m
                                """).collect()
    if dataframe[0][0] != 0:
        v_COMPARISON_ID = session.sql(f"select COMPARISON_ID from {database_name}.METADATA.COUNT_COMPARISON_RESULTS order by COMPARISON_ID desc limit 1").collect()
        if v_COMPARISON_ID[0][0] !=0:
            final_data = session.sql(f"""select  COMPARISON_ID, 
                                            BRONZE_TABLE_NAME, 
                                            SILVER_TABLE_NAME, 
                                            BRONZE_COUNT,
                                            SILVER_COUNT, 
                                            STATUS
                    from {database_name}.METADATA.COUNT_COMPARISON_RESULTS 
                    where COMPARISON_ID = {v_COMPARISON_ID[0][0]} 
                     """)
            f_fd = final_data.to_pandas().to_json(orient="records")
            html_content = GENERATE_HTML_TABLE(f_fd)
            escaped_html = html_content.replace("'", "''")  # Escape single quotes for SQL compatibility
            
            session.sql(f"""call SYSTEM$SEND_EMAIL(
                                'MAIL_NOTIFICATION',
                                'sunilreddy559@gmail.com',
                                '{database_name} : Bronze to Silver Tables Comparison Report || {current_datetime} ',
                                '{escaped_html}',
                                'text/html')
                        """).collect()
            session.sql(f""""
            update {database_name}.METADATA.COUNT_COMPARISON_RESULTS
            set IS_ALERT_SENT = 'TRUE'
            WHERE COMPARISON_ID = {unique_id}
            """).collect()
            return 'SUCCESS'
    else:
        return 'No records'
$$;