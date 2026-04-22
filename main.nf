#!/usr/bin/env nextflow

/*
 * nf-test-asserter
 *
 * Evaluates nf-test pipeline-test assertions on a Seqera Platform compute
 * environment against the already-published outputs of a previous (phase-1)
 * workflow run, and emits only the regenerated <testFile>.nf.test.snap.
 *
 * Does NOT re-run the pipeline. Uses a patched nf-test with
 * `--skip-nextflow-run`, `--workflow-trace-csv`, and `--stage-outputs-from`
 * so the assertion block evaluates against pre-staged output files and a
 * synthesised trace.csv.
 *
 * Expected params:
 *   --pipeline_repo    git URL of the pipeline under test
 *   --pipeline_rev     git revision (branch, tag, or commit SHA)
 *   --test_file        path (relative to repo root) of the .nf.test file
 *   --outdir_uri       URI of phase-1's published outputs (staged by Nextflow)
 *   --workflow_id      Platform workflow ID of phase-1 (used to fetch tasks)
 *   --api_endpoint     Platform API endpoint (default https://api.cloud.seqera.io)
 *   --workspace_id     Platform workspace numeric ID
 *   --test_profile     nf-test / Nextflow profile (default 'test')
 *   --outdir           where this pipeline publishes the regenerated .snap
 */

params.pipeline_repo = null
params.pipeline_rev  = null
params.test_file     = null
params.outdir_uri    = null
params.workflow_id   = null
params.api_endpoint  = 'https://api.cloud.seqera.io'
params.workspace_id  = null
params.test_profile  = 'test'
params.outdir        = null

process ASSERT_SNAPSHOT {

    container 'community.wave.seqera.io/library/nf-test-asserter:c757b99762ca8292'

    publishDir params.outdir, mode: 'copy'

    input:
    path patched_jar
    path staged_outputs, stageAs: 'staged_outputs'
    val pipeline_repo
    val pipeline_rev
    val test_file
    val workflow_id
    val api_endpoint
    val workspace_id
    val test_profile

    output:
    path "*.nf.test.snap", optional: true, emit: snap
    path "asserter.log",                    emit: log

    script:
    """
    set -euo pipefail

    # Clone the pipeline under test at the exact revision.
    mkdir -p pipeline
    git clone --no-checkout '${pipeline_repo}' pipeline
    ( cd pipeline && git checkout '${pipeline_rev}' )

    # Build a Nextflow-format trace.csv from Platform's task list so
    # workflow.trace.succeeded() is a real number.
    python3 <<'PY'
import json, os, urllib.request, csv

token    = os.environ.get('TOWER_ACCESS_TOKEN', '')
endpoint = "${api_endpoint}".rstrip('/')
ws       = "${workspace_id}"
wf       = "${workflow_id}"

def get(url):
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())

tasks = []
offset = 0
while True:
    url = f"{endpoint}/workflow/{wf}/tasks?workspaceId={ws}&max=100&offset={offset}"
    body = get(url)
    page = body.get('tasks', [])
    tasks.extend(page)
    if len(page) < 100:
        break
    offset += len(page)

with open('trace.csv', 'w', newline='') as f:
    w = csv.writer(f, delimiter='\\t')
    w.writerow(['task_id','hash','native_id','name','status','exit','submit','start','complete','duration','realtime','%cpu','peak_rss','peak_vmem','rchar','wchar'])
    for i, t in enumerate(tasks, 1):
        row = t.get('task', t)
        w.writerow([
            row.get('taskId', i),
            row.get('hash', ''),
            row.get('nativeId', ''),
            row.get('name', row.get('process', '')),
            row.get('status', 'COMPLETED'),
            row.get('exit', 0),
            row.get('submit', ''),
            row.get('start', ''),
            row.get('complete', ''),
            row.get('duration', ''),
            row.get('realtime', ''),
            row.get('pcpu', ''),
            row.get('peakRss', ''),
            row.get('peakVmem', ''),
            row.get('rchar', ''),
            row.get('wchar', ''),
        ])
print(f"Wrote trace.csv with {len(tasks)} tasks")
PY

    STAGED_ABS="\$(readlink -f staged_outputs)"
    TRACE_ABS="\$(readlink -f trace.csv)"

    cd pipeline

    java -jar ../${patched_jar.name} test '${test_file}' \\
        --update-snapshot \\
        --skip-nextflow-run \\
        --workflow-trace-csv "\$TRACE_ABS" \\
        --stage-outputs-from "\$STAGED_ABS" \\
        --profile=+${test_profile} \\
        --verbose \\
        > ../asserter.log 2>&1 || true

    cd ..

    # Surface the .snap next to the test file (whether nf-test rewrote it or
    # the committed one matched). For regeneration scenarios, the caller can
    # optionally clean-snapshot via --clean-snapshot (set in options) before
    # invocation.
    SNAP_PATH="pipeline/${test_file}.snap"
    if [ -f "\$SNAP_PATH" ]; then
        cp "\$SNAP_PATH" "\$(basename '${test_file}').snap"
    fi
    """
}

workflow {
    if (!params.pipeline_repo || !params.pipeline_rev || !params.test_file
            || !params.outdir_uri || !params.workflow_id || !params.workspace_id
            || !params.outdir) {
        error "Missing required params. See main.nf header for the full list."
    }
    patched_jar    = file("${projectDir}/bin/nf-test-patched.jar", checkIfExists: true)
    staged_outputs = file("${params.outdir_uri}")
    ASSERT_SNAPSHOT(
        patched_jar,
        staged_outputs,
        params.pipeline_repo,
        params.pipeline_rev,
        params.test_file,
        params.workflow_id,
        params.api_endpoint,
        params.workspace_id,
        params.test_profile,
    )
}
