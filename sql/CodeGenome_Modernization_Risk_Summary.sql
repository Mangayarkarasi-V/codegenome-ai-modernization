SELECT
    risk_level,
    COUNT(*) AS total_programs
FROM workspace.default.codegenome_modernization_score
GROUP BY risk_level
ORDER BY total_programs DESC;