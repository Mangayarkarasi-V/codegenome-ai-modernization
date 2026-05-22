SELECT
    file_type,
    COUNT(*) AS total_files
FROM workspace.default.codegenome_files
GROUP BY file_type
ORDER BY total_files DESC;