# microdados_validation_extended.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import shutil
import json
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from openai import OpenAI

# === Paths (dentro do container) ===
FILE_PATH = "/opt/airflow/data/microdados_2022.CSV"
VALIDATED_DIR = "/opt/airflow/data/validated"
QUARANTINE_DIR = "/opt/airflow/data/quarantine"
REPORTS_DIR = "/opt/airflow/data/reports"

# === LLM via Ollama (host Windows) ===
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.1")
LLM_URL_RAW = os.getenv("LLM_BASE_URL", "http://host.docker.internal:11434/v1")
LLM_URL = LLM_URL_RAW.rstrip("/")
if not LLM_URL.endswith("/v1"):
    LLM_URL = f"{LLM_URL}/v1"

# === E-mail de destino ===
EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "brennoewerton1@gmail.com")


def run_gx_checkpoint(**context):
    ctx = gx.get_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_runtime_asset_name",
        runtime_parameters={"path": FILE_PATH},
        batch_identifiers={"runtime_batch_identifier_name": "microdados_2022"},
        batch_spec_passthrough={
            "reader_method": "read_csv",
            "reader_options": {"encoding": "latin1", "sep": ";"},
        },
    )

    checkpoint_name = "microdados_2022_checkpoint"
    if checkpoint_name not in ctx.list_checkpoints():
        ctx.add_checkpoint(
            name=checkpoint_name,
            config_version=1.0,
            class_name="Checkpoint",
            validations=[{
                "batch_request": batch_request,
                "expectation_suite_name": "microdados_suite",
            }],
        )

    result = ctx.run_checkpoint(checkpoint_name=checkpoint_name)
    success = result["success"]

    os.makedirs(REPORTS_DIR, exist_ok=True)
    result_path = os.path.join(
        REPORTS_DIR, f"gx_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)

    # Extrai falhas (para eventual diagnóstico)
    failed = []
    run_results = result.get("run_results", {})
    for _, rr in run_results.items():
        vr = rr.get("validation_result") or {}
        for r in vr.get("results", []):
            if not r.get("success", False):
                exp = r.get("expectation_config", {})
                failed.append({
                    "expectation_type": exp.get("expectation_type"),
                    "kwargs": exp.get("kwargs", {}),
                })

    ti = context["ti"]
    ti.xcom_push(key="validation_success", value=success)
    ti.xcom_push(key="validation_result_path", value=result_path)
    ti.xcom_push(key="failed_expectations", value=failed)

    print(f"✅ Validation success={success} | JSON: {result_path} | Falhas: {len(failed)}")
    return success


def analyze_results_with_llm(**context):
    ti = context["ti"]
    validation_path = ti.xcom_pull(
        key="validation_result_path", task_ids="run_gx_checkpoint"
    )

    with open(validation_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    if isinstance(loaded, str):
        result_json = json.loads(loaded)
    else:
        result_json = loaded

    failed = []
    run_results = result_json.get("run_results") if isinstance(result_json, dict) else None
    if isinstance(run_results, dict) and run_results:
        for _key, payload in run_results.items():
            validation_result = (payload or {}).get("validation_result", {})
            results_list = validation_result.get("results", [])
            for r in results_list:
                if not r.get("success", False):
                    failed.append(r)
    if not failed and isinstance(result_json, dict) and "results" in result_json:
        for r in result_json.get("results", []):
            if not r.get("success", False):
                failed.append(r)

    if not failed:
        print("✅ Nenhuma falha detectada; nada a sugerir.")
        ti.xcom_push(key="report_path", value=None)
        return None

    lines = []
    for r in failed:
        cfg = r.get("expectation_config", {})
        etype = cfg.get("expectation_type", "unknown_expectation")
        kwargs = cfg.get("kwargs", {})
        res = r.get("result", {})
        extras = []
        for k in ("observed_value", "unexpected_count", "unexpected_percent", "partial_unexpected_list"):
            if k in res:
                extras.append(f"{k}={res[k]}")
        extra_txt = f" | {'; '.join(extras)}" if extras else ""
        lines.append(f"- {etype}: {kwargs}{extra_txt}")
    summary = "\n".join(lines)

    prompt = (
        "Você é um especialista em qualidade de dados.\n"
        "Aqui estão as expectativas do Great Expectations que falharam:\n"
        f"{summary}\n\n"
        "Gere um diagnóstico breve e sugestões práticas para que cada regra atinja 100% de sucesso.\n"
        "Seja objetivo e use bullet points."
    )

    client = OpenAI(base_url=LLM_URL, api_key="ollama")
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    suggestions = resp.choices[0].message.content

    os.makedirs(REPORTS_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(REPORTS_DIR, f"gx_suggestions_{ts}.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Relatório de Qualidade dos Dados\n\n")
        f.write(f"**Arquivo validado:** {os.path.basename(FILE_PATH)}\n\n")
        f.write("## Expectativas com falha\n")
        f.write(summary + "\n\n")
        f.write("## Sugestões (LLM)\n")
        f.write(suggestions + "\n")

    ti.xcom_push(key="report_path", value=report_path)
    print(f"📝 Relatório de sugestões salvo em: {report_path}")
    return report_path


def move_file(**context):
    ti = context["ti"]
    success = ti.xcom_pull(key="validation_success", task_ids="run_gx_checkpoint")
    target_dir = VALIDATED_DIR if success else QUARANTINE_DIR
    os.makedirs(target_dir, exist_ok=True)

    dest_path = os.path.join(target_dir, os.path.basename(FILE_PATH))
    if os.path.exists(FILE_PATH):
        shutil.move(FILE_PATH, dest_path)
        print(f"📦 Arquivo movido para: {dest_path}")
    else:
        print(f"⚠️ Arquivo {FILE_PATH} não encontrado para mover (já movido?).")

    ti.xcom_push(key="final_path", value=dest_path)
    return dest_path


def prepare_email_body(**context):
    ti = context["ti"]
    success = ti.xcom_pull(key="validation_success", task_ids="run_gx_checkpoint")
    report_path = ti.xcom_pull(key="report_path", task_ids="analyze_results_with_llm")
    final_path = ti.xcom_pull(key="final_path", task_ids="move_file_task")

    if success:
        subject = "✅ Validação OK — Microdados"
        body = (
            f"<p>O arquivo <b>{os.path.basename(FILE_PATH)}</b> passou em todas as validações.</p>"
            f"<p>Arquivo movido para: <code>{final_path}</code></p>"
        )
    else:
        subject = "⚠️ Falhas na Validação — Microdados"
        extra = f"<p>Relatório com sugestões: <code>{report_path}</code></p>" if report_path else ""
        body = (
            f"<p>Foram encontradas falhas na validação de <b>{os.path.basename(FILE_PATH)}</b>.</p>"
            f"<p>Arquivo movido para: <code>{final_path}</code></p>"
            f"{extra}"
        )

    ti.xcom_push(key="email_subject", value=subject)
    ti.xcom_push(key="email_body", value=body)
    return {"subject": subject, "body": body}


def finalize_email(**context):
    ti = context["ti"]
    base = ti.xcom_pull(task_ids="prepare_email_body")
    subject = base.get("subject", "Validação — Microdados")
    body_html = base.get("body", "")

    report_path = ti.xcom_pull(key="report_path", task_ids="analyze_results_with_llm")

    attachments = []
    if report_path and os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                suggestions_md = f.read()
            suggestions_html = "<pre style='white-space:pre-wrap'>" + suggestions_md + "</pre>"
            body_html += (
                "<hr><h3>Sugestões geradas pela LLM</h3>"
                "<p>Segue o conteúdo abaixo (o arquivo completo também vai em anexo):</p>"
                f"{suggestions_html}"
            )
            attachments = [report_path]
        except Exception as e:
            body_html += f"<p><i>Não foi possível anexar o relatório: {e}</i></p>"

    ti.xcom_push(key="final_email_subject", value=subject)
    ti.xcom_push(key="final_email_body", value=body_html)
    ti.xcom_push(key="attachments", value=attachments)
    return {"subject": subject, "body": body_html, "attachments": attachments}


def branch_success_or_fail(**context):
    """Desvia o fluxo: se sucesso, encerra; se falha, segue para LLM/e-mail."""
    ti = context["ti"]
    success = ti.xcom_pull(key="validation_success", task_ids="run_gx_checkpoint")
    return "end_success" if success else "analyze_results_with_llm"


with DAG(
    "microdados_validation",
    start_date=datetime(2025, 10, 6),
    schedule_interval=None,
    catchup=False,
    tags=["great_expectations", "data_quality", "llm"],
    render_template_as_native_obj=True,  # para o files aceitar lista nativa
) as dag:

    run_checkpoint_task = PythonOperator(
        task_id="run_gx_checkpoint",
        python_callable=run_gx_checkpoint,
        provide_context=True,
    )

    move_file_task = PythonOperator(
        task_id="move_file_task",
        python_callable=move_file,
        provide_context=True,
    )

    # Branch após mover o arquivo: sucesso -> encerra; falha -> LLM/e-mail
    branch_task = BranchPythonOperator(
        task_id="branch_success_or_fail",
        python_callable=branch_success_or_fail,
        provide_context=True,
    )

    end_success = EmptyOperator(task_id="end_success")

    analyze_results_task = PythonOperator(
        task_id="analyze_results_with_llm",
        python_callable=analyze_results_with_llm,
        provide_context=True,
    )

    prepare_email_task = PythonOperator(
        task_id="prepare_email_body",
        python_callable=prepare_email_body,
        provide_context=True,
    )

    finalize_email_task = PythonOperator(
        task_id="finalize_email",
        python_callable=finalize_email,
        provide_context=True,
    )

    send_email_task = EmailOperator(
        task_id="send_email_task",
        to=EMAIL_TO,
        subject="{{ ti.xcom_pull(task_ids='finalize_email', key='final_email_subject') }}",
        html_content="{{ ti.xcom_pull(task_ids='finalize_email', key='final_email_body') }}",
        files="{{ ti.xcom_pull(task_ids='finalize_email', key='attachments') }}",
    )

    # Orquestração:
    # Sempre: checkpoint -> move file -> branch
    run_checkpoint_task >> move_file_task >> branch_task

    # Sucesso: termina aqui
    branch_task >> end_success

    # Falha: roda LLM + e-mail
    branch_task >> analyze_results_task >> prepare_email_task >> finalize_email_task >> send_email_task
