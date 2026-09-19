use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use pgqueue::Queue;

struct AppState {
    queue: Queue,
}

async fn index(state: web::Data<AppState>) -> impl Responder {
    let stats = match state.queue.stats(None).await {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().body(format!("{e}")),
    };
    let jobs = match state.queue.recent_jobs(None, 100).await {
        Ok(j) => j,
        Err(e) => return HttpResponse::InternalServerError().body(format!("{e}")),
    };

    let mut stats_rows = String::new();
    for s in &stats {
        stats_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            s.queue, s.queued, s.running, s.done, s.failed, s.dead
        ));
    }

    let mut job_rows = String::new();
    for j in &jobs {
        let retry_btn = if j.status == "dead" {
            format!(
                r#"<form method="post" action="/jobs/{}/retry"><button type="submit">retry</button></form>"#,
                j.id
            )
        } else {
            String::new()
        };
        job_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td><td>{}</td><td>{}</td></tr>",
            j.id,
            j.queue,
            j.job_type,
            j.status,
            j.attempts,
            j.max_attempts,
            j.last_error.as_deref().unwrap_or(""),
            retry_btn
        ));
    }

    let html = format!(
        r#"<!doctype html>
<html><head><title>pgqueue</title>
<style>
body {{ font-family: monospace; margin: 2rem; }}
table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
td, th {{ border: 1px solid #ccc; padding: 4px 8px; text-align: left; }}
</style>
</head><body>
<h1>pgqueue</h1>
<h2>Queues</h2>
<table><tr><th>queue</th><th>queued</th><th>running</th><th>done</th><th>failed</th><th>dead</th></tr>{stats_rows}</table>
<h2>Recent jobs (last 100)</h2>
<table><tr><th>id</th><th>queue</th><th>type</th><th>status</th><th>attempts</th><th>last_error</th><th></th></tr>{job_rows}</table>
</body></html>"#
    );
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

async fn api_stats(state: web::Data<AppState>) -> impl Responder {
    match state.queue.stats(None).await {
        Ok(s) => HttpResponse::Ok().json(s),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

async fn api_jobs(state: web::Data<AppState>) -> impl Responder {
    match state.queue.recent_jobs(None, 200).await {
        Ok(j) => HttpResponse::Ok().json(j),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

async fn retry_job(state: web::Data<AppState>, path: web::Path<i64>) -> impl Responder {
    let job_id = path.into_inner();
    match state.queue.requeue_dead(job_id).await {
        Ok(true) => HttpResponse::SeeOther()
            .append_header(("Location", "/"))
            .finish(),
        Ok(false) => HttpResponse::NotFound().body("job not found or not dead"),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

pub async fn run(queue: Queue, port: u16) -> std::io::Result<()> {
    let data = web::Data::new(AppState { queue });
    log::info!("pgqueue dashboard listening on http://127.0.0.1:{port}");
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/", web::get().to(index))
            .route("/api/stats", web::get().to(api_stats))
            .route("/api/jobs", web::get().to(api_jobs))
            .route("/jobs/{id}/retry", web::post().to(retry_job))
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
