import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime

st.set_page_config(
    page_title="CodeGenome AI",
    page_icon="🔥",
    layout="wide"
)

WORKFLOW_URL = "PASTE_YOUR_ZABBIX_POWER_AUTOMATE_WORKFLOW_URL_HERE"

def send_zabbix_teams_alert(filtered_alerts):
    alert_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    alert_lines = "\n".join(
        [f"- {row.file_name} | {row.severity} | {row.alert_type}" for row in filtered_alerts.itertuples()]
    )

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "🚨 CodeGenome AI Modernization Governance Alert",
                            "weight": "Bolder",
                            "size": "Large",
                            "color": "Attention"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Source", "value": "CodeGenome AI"},
                                {"title": "Connector", "value": "Zabbix / Power Automate / Teams"},
                                {"title": "Alert Time", "value": alert_time},
                                {"title": "Total Alerts", "value": str(len(filtered_alerts))},
                                {"title": "Recommendation", "value": "Perform phased modernization and dependency remediation."}
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "Alert Details",
                            "weight": "Bolder",
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": alert_lines if alert_lines else "No active alerts.",
                            "wrap": True
                        }
                    ]
                }
            }
        ]
    }

    response = requests.post(WORKFLOW_URL, json=payload)
    return response.status_code, response.text


# -----------------------------
# Theme
# -----------------------------
st.markdown("""
<style>
.stApp { background-color: #020617; color: #F8FAFC; }
section[data-testid="stSidebar"] { background-color: #0F172A; }
section[data-testid="stSidebar"] * { color: #F8FAFC !important; }
.block-container { padding-top: 1.5rem; }

div[data-testid="metric-container"] {
    background: linear-gradient(135deg,#111827,#1E293B);
    border: 1px solid #334155;
    padding: 18px;
    border-radius: 14px;
    box-shadow: 0px 0px 12px rgba(56,189,248,0.18);
}

div[data-testid="metric-container"] label,
div[data-testid="metric-container"] div {
    color: #F8FAFC !important;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Header
# -----------------------------
st.markdown("""
<div style="
background:linear-gradient(90deg,#0F172A,#1E293B);
padding:25px;
border-radius:18px;
border:1px solid #334155;
margin-bottom:25px;
display:flex;
align-items:center;
gap:20px;
">
<img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png"
width="90"
style="background:white;padding:8px;border-radius:10px;">

<div>
<h1 style="margin:0;color:#FFFFFF;font-size:36px;font-weight:800;">
🔥 CodeGenome AI Modernization Intelligence Hub
</h1>
<p style="margin-top:8px;color:#CBD5E1;font-size:17px;">
AI-Powered Enterprise Mainframe Modernization Platform
</p>
</div>
</div>
""", unsafe_allow_html=True)

file_df = pd.DataFrame({
    "file_type": ["CBL", "JCL", "SQL", "CPY", "CSV"],
    "count": [18, 6, 6, 6, 2]
})

risk_df = pd.DataFrame({
    "risk_level": ["Low", "Medium", "High"],
    "programs": [31, 5, 2]
})

db2_df = pd.DataFrame({
    "operation": ["SELECT", "UPDATE", "INSERT"],
    "count": [3, 3, 1]
})

jcl_df = pd.DataFrame({
    "job_name": ["JOB001", "MONTHEND", "NIGHTLY", "YEARCLOSE"],
    "steps": [4, 2, 3, 1],
    "complexity": ["High", "Medium", "Medium", "Low"]
})

alerts_df = pd.DataFrame({
    "severity": ["CRITICAL", "CRITICAL", "HIGH", "HIGH"],
    "file_name": ["PAYROLL.cbl", "JOB001.jcl", "CUSTOMER.cbl", "FUNDSXFER.cbl"],
    "alert_type": [
        "High modernization risk",
        "Complex JCL orchestration",
        "DB2 write operation",
        "Dependency complexity"
    ],
    "recommendation": [
        "Plan phased modernization",
        "Review batch execution flow",
        "Validate DB2 impact",
        "Review dependency remediation"
    ]
})

chart_template = "plotly_dark"

st.sidebar.title("📌 Navigation")

section = st.sidebar.radio(
    "Select Module",
    [
        "Executive Overview",
        "Risk Assessment",
        "DB2 Lineage",
        "JCL Orchestration",
        "AI Risk Alerts",
        "Architecture",
        "Ask CodeGenome AI"
    ]
)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Assets", "38")
col2.metric("DB2 Tables", "5")
col3.metric("JCL Jobs", "6")
col4.metric("High Risk", "2")
col5.metric("API Candidates", "15")

st.divider()

if section == "Executive Overview":
    st.subheader("📊 Executive Overview")

    c1, c2 = st.columns(2)

    with c1:
        fig = px.pie(file_df, names="file_type", values="count", hole=0.45,
                     title="File Type Distribution", template=chart_template)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.bar(risk_df, x="risk_level", y="programs", color="risk_level",
                     title="Modernization Risk Distribution", template=chart_template)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### AI Executive Summary")
    st.success("38 legacy assets analyzed. 2 high-risk modernization blockers identified. 15 assets are suitable for API/cloud modernization.")
    st.dataframe(file_df, use_container_width=True)

elif section == "Risk Assessment":
    st.subheader("🔥 Modernization Risk Assessment")

    selected_risk = st.multiselect(
        "Filter Risk Level",
        risk_df["risk_level"].tolist(),
        default=risk_df["risk_level"].tolist()
    )

    filtered_risk = risk_df[risk_df["risk_level"].isin(selected_risk)]

    fig = px.bar(filtered_risk, x="risk_level", y="programs", color="risk_level",
                 title="Filtered Risk Level Summary", template=chart_template)
    st.plotly_chart(fig, use_container_width=True)

    st.warning("High-risk programs require phased modernization, DB2 impact review, and regression validation.")

elif section == "DB2 Lineage":
    st.subheader("🗄️ DB2 Lineage Intelligence")

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(db2_df, x="operation", y="count", color="operation",
                     title="DB2 CRUD Operation Summary", template=chart_template)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.pie(db2_df, names="operation", values="count", hole=0.45,
                     title="DB2 Operation Distribution", template=chart_template)
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(db2_df, use_container_width=True)

elif section == "JCL Orchestration":
    st.subheader("⚙️ JCL Batch Orchestration")

    fig = px.bar(jcl_df, x="job_name", y="steps", color="complexity",
                 title="JCL Job Complexity", template=chart_template)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(jcl_df, use_container_width=True)

elif section == "AI Risk Alerts":
    st.subheader("🚨 AI Risk Alert Center")

    severity = st.selectbox("Filter Severity", ["ALL", "CRITICAL", "HIGH"])

    filtered_alerts = alerts_df.copy()

    if severity != "ALL":
        filtered_alerts = filtered_alerts[filtered_alerts["severity"] == severity]

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric("Total Alerts", len(filtered_alerts))

    with c2:
        st.metric("Critical Alerts", len(alerts_df[alerts_df["severity"] == "CRITICAL"]))

    with c3:
        st.metric("Teams Connector", "Enabled")

    fig = px.histogram(filtered_alerts, x="severity", color="severity",
                       title="Alert Severity Distribution", template=chart_template)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(filtered_alerts, use_container_width=True)

    st.markdown("### Microsoft Teams Governance Notification")
    st.info("Send selected CodeGenome AI alerts to Microsoft Teams through the Zabbix / Power Automate connector.")

    if st.button("🚨 Send Critical Alert to Teams via Zabbix Connector"):
        if WORKFLOW_URL == "PASTE_YOUR_ZABBIX_POWER_AUTOMATE_WORKFLOW_URL_HERE":
            st.error("Please update WORKFLOW_URL in app.py before sending alerts.")
        else:
            status_code, response_text = send_zabbix_teams_alert(filtered_alerts)

            if status_code in [200, 202]:
                st.success(f"Alert sent successfully to Microsoft Teams. Status Code: {status_code}")
            else:
                st.error("Failed to send alert to Teams.")
                st.write("Status Code:", status_code)
                st.write(response_text)

elif section == "Architecture":
    st.subheader("🏗️ CodeGenome AI Architecture")

    st.markdown("""
    ### Platform Flow

    **Mainframe Legacy Assets**  
    COBOL | JCL | DB2 SQL | Copybooks | CSV  

    ⬇️  

    **Databricks Lakehouse Platform**  
    File Upload | Delta Lake | Unity Catalog | Serverless SQL  

    ⬇️  

    **AI-Powered Metadata Extraction Engine**  
    Dependency Parsing | DB2 Lineage | JCL Flow | Business Rules  

    ⬇️  

    **Modernization Intelligence Layer**  
    Readiness Scoring | Risk Classification | AI Recommendations  

    ⬇️  

    **AI Risk Alert Engine**  
    Governance Alerts | Zabbix Connector | Power Automate | Microsoft Teams  

    ⬇️  

    **CodeGenome AI Intelligence Hub**  
    Dashboards | Genie | Streamlit App | Risk Alerts
    """)

    st.info("This architecture enables continuous modernization governance and AI-driven legacy transformation.")

elif section == "Ask CodeGenome AI":
    st.subheader("🤖 Ask CodeGenome AI")

    sample_questions = [
        "Which programs are high risk?",
        "Show DB2 lineage",
        "Which JCL job is most complex?",
        "Which assets are API candidates?",
        "Generate modernization summary"
    ]

    st.markdown("### Try Sample Questions")
    st.write(sample_questions)

    question = st.text_input("Ask a modernization question")

    if question:
        q = question.lower()

        if "high risk" in q:
            st.error("High-risk programs identified: PAYROLL.cbl and JOB001.jcl.")
        elif "db2" in q:
            st.info("DB2 lineage includes SELECT, UPDATE and INSERT operations across key enterprise tables.")
        elif "jcl" in q:
            st.warning("JOB001 is the most complex JCL job with multiple execution steps.")
        elif "api" in q or "cloud" in q:
            st.success("15 assets are identified as API/cloud modernization candidates.")
        elif "summary" in q:
            st.success("CodeGenome AI analyzed 38 assets, identified 2 high-risk programs, 5 DB2 tables, 6 JCL jobs, and 15 API modernization candidates.")
        else:
            st.info("CodeGenome AI can answer modernization risk, DB2 lineage, JCL flow, dependency, business rule, and alert questions.")