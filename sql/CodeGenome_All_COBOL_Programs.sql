SELECT
    file_name,
    file_type
FROM workspace.default.codegenome_files
WHERE UPPER(file_type) = 'CBL'
ORDER BY file_name;