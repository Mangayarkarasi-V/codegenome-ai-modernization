SELECT
    job_name,
    step_name,
    program_name,
    datasets
FROM workspace.default.codegenome_jcl_flow
ORDER BY job_name, step_name;