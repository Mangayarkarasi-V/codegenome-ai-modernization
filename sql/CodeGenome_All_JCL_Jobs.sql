SELECT
    file_name,
    job_name,
    step_name,
    program_name,
    dataset_count,
    status
FROM workspace.default.codegenome_jcl_flow
ORDER BY file_name, step_name;