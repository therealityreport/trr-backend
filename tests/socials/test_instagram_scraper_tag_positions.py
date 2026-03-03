from __future__ import annotations

from trr_backend.socials.instagram.scraper import InstagramScraper


def test_extract_tagged_users_detail_includes_graphql_xy_coordinates() -> None:
    scraper = InstagramScraper(cookies={})
    node = {
        "edge_media_to_tagged_user": {
            "edges": [
                {
                    "node": {
                        "user": {
                            "username": "person_one",
                            "id": "1",
                            "full_name": "Person One",
                        },
                        "x": 0.33333,
                        "y": 0.66666,
                    }
                }
            ]
        }
    }

    details = scraper._extract_tagged_users_detail(node)  # noqa: SLF001
    assert len(details) == 1
    assert details[0].username == "person_one"
    assert details[0].tag_x == 0.3333
    assert details[0].tag_y == 0.6667
    assert details[0].tag_position_source == "graphql_node.xy"


def test_extract_tagged_users_detail_includes_rest_position_coordinates() -> None:
    scraper = InstagramScraper(cookies={})
    node = {
        "usertags": {
            "in": [
                {
                    "user": {"username": "rest_array"},
                    "position": [0.25, 0.75],
                },
                {
                    "user": {"username": "rest_object"},
                    "position": {"x": 0.9, "y": 0.1},
                },
                {
                    "user": {"username": "rest_invalid"},
                    "position": {"x": "x", "y": "y"},
                },
            ]
        }
    }

    details = scraper._extract_tagged_users_detail(node)  # noqa: SLF001
    assert len(details) == 3
    assert details[0].tag_x == 0.25
    assert details[0].tag_y == 0.75
    assert details[0].tag_position_source == "rest_usertags.position_array"
    assert details[1].tag_x == 0.9
    assert details[1].tag_y == 0.1
    assert details[1].tag_position_source == "rest_usertags.position_object"
    assert details[2].tag_x is None
    assert details[2].tag_y is None
    assert details[2].tag_position_source is None


def test_extract_child_posts_data_includes_slide_index_and_tag_positions() -> None:
    scraper = InstagramScraper(cookies={})
    node = {
        "carousel_media": [
            {
                "original_width": 1080,
                "original_height": 1350,
                "image_versions2": {"candidates": [{"url": "https://images.test/slide-1.jpg"}]},
                "usertags": {
                    "in": [
                        {
                            "user": {"username": "slide_one_tag"},
                            "position": [0.4, 0.6],
                        }
                    ]
                },
            },
            {
                "original_width": 1080,
                "original_height": 1350,
                "image_versions2": {"candidates": [{"url": "https://images.test/slide-2.jpg"}]},
            },
        ]
    }

    children = scraper._extract_child_posts_data(node)  # noqa: SLF001
    assert len(children) == 2
    assert children[0]["slide_index"] == 0
    assert children[0]["display_url"] == "https://images.test/slide-1.jpg"
    assert len(children[0]["tagged_users_detail"]) == 1
    assert children[0]["tagged_users_detail"][0]["username"] == "slide_one_tag"
    assert children[0]["tagged_users_detail"][0]["tag_x"] == 0.4
    assert children[0]["tagged_users_detail"][0]["tag_y"] == 0.6
    assert children[1]["slide_index"] == 1
    assert children[1]["tagged_users_detail"] == []
