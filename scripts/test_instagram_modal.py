"""Quick test: does Instagram GraphQL work from Modal with fast_mode?"""
import modal
import json
import sys
sys.path.insert(0, ".")
from trr_backend.modal_jobs import app as main_app, _FUNCTION_IMAGE_BINDINGS, _secrets

app = modal.App("trr-instagram-test")

@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    timeout=90,
)
def test_instagram_graphql():
    import os, json
    from trr_backend.socials.instagram import InstagramScraper
    from trr_backend.socials.control_plane import _load_instagram_cookies

    cookies = _load_instagram_cookies()
    result = {
        "has_sessionid": bool(cookies.get("sessionid")),
        "has_csrftoken": bool(cookies.get("csrftoken")),
    }

    # Test 1: Public scraper, fast_mode=True
    pub = InstagramScraper(cookies={}, browser_account_id="bravotv")
    d1 = pub.fetch_posts_graphql("bravotv", None, 0.5, fast_mode=True, page_size=50)
    conn1 = ((d1 or {}).get("data") or {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
    result["public_fast"] = {
        "has_data": d1 is not None,
        "has_edges": bool(conn1.get("edges")),
        "edge_count": len(conn1.get("edges") or []),
        "meta": {k: v for k, v in (getattr(pub, "last_retrieval_meta", {}) or {}).items() if k in ("error_code", "error_class", "error_status_code", "error_message", "total_posts", "transport")},
    }

    # Test 2: Public scraper, fast_mode=False
    pub2 = InstagramScraper(cookies={}, browser_account_id="bravotv")
    d2 = pub2.fetch_posts_graphql("bravotv", None, 0.5, fast_mode=False, page_size=50)
    conn2 = ((d2 or {}).get("data") or {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
    result["public_normal"] = {
        "has_data": d2 is not None,
        "has_edges": bool(conn2.get("edges")),
        "edge_count": len(conn2.get("edges") or []),
        "meta": {k: v for k, v in (getattr(pub2, "last_retrieval_meta", {}) or {}).items() if k in ("error_code", "error_class", "error_status_code", "error_message", "total_posts", "transport")},
    }

    # Test 3: Auth scraper, fast_mode=True
    if cookies.get("sessionid"):
        auth = InstagramScraper(cookies=cookies, browser_account_id="bravotv")
        d3 = auth.fetch_posts_graphql("bravotv", None, 0.5, fast_mode=True, page_size=50)
        conn3 = ((d3 or {}).get("data") or {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
        result["auth_fast"] = {
            "has_data": d3 is not None,
            "has_edges": bool(conn3.get("edges")),
            "edge_count": len(conn3.get("edges") or []),
            "meta": {k: v for k, v in (getattr(auth, "last_retrieval_meta", {}) or {}).items() if k in ("error_code", "error_class", "error_status_code", "error_message", "total_posts", "transport")},
        }

    return result


if __name__ == "__main__":
    with app.run():
        r = test_instagram_graphql.remote()
        print(json.dumps(r, indent=2, default=str))
