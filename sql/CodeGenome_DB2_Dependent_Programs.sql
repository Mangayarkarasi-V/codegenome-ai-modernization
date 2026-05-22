SELECT
    file_name,
    readiness_score,
    risk_factors
FROM workspace.default.codegenome_modernization_score
WHERE UPPER(risk_factors) LIKE '%DB2%'
ORDER BY readiness_score ASC;