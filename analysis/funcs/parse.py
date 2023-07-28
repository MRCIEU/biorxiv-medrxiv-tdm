from pathlib import Path
from typing import Any, Dict, Optional
from zipfile import ZipFile

import xmltodict
from bs4 import BeautifulSoup as bs
from lxml import etree
from pydash import py_

from yiutils.failsafe import failsafe  # isort:skip

from funcs.paths import paths  # isort:skip


@failsafe()
def main_extract(input_zip: Path) -> Dict[str, Any]:
    # read zip
    zip_path = str(input_zip.relative_to(paths["raw_data_dir"]))

    with ZipFile(input_zip, "r") as zip:
        # read manifest
        manifest_file = "manifest.xml"
        zip_data = zip.read(manifest_file)
        bs_content = bs(zip_data, "xml")
        find_res = bs_content.find_all("instance")
        full_text_file = find_res[0].attrs["href"].replace("\\", "/")

        # read fulltext
        zip_data = zip.read(full_text_file)
        bs_content = bs(zip_data, "xml")

    # fulltext
    full_text_dict = xmltodict.parse(str(bs_content))
    parse_res = parse_full_text(
        full_text_dict=full_text_dict,
        full_text_bs=bs_content,
    )

    res = {
        "zip_path": zip_path,
        "full_text_file": full_text_file,
        "doi": parse_res["doi"],
        "title": parse_res["title"],
        "version": parse_res["version"],
        "category": parse_res["category"],
        "year_month": parse_res["year_month"],
        "preprint_source": parse_res["preprint_source"],
        "publish_destination": parse_res["publish_destination"],
    }
    return res


def parse_full_text(
    full_text_dict: Dict[str, Any], full_text_bs: bs, verbose: bool = False
) -> Dict[str, Any]:
    # doi
    doi_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "article-meta", "article-id"])
        .filter(lambda col: col["@pub-id-type"] == "doi")
        .value()[0]
    )
    assert isinstance(doi_find, dict), {"doi_find": doi_find}
    doi = doi_find["#text"]
    assert isinstance(doi, str), {"doi": doi}

    # title
    title_find = (
        py_.chain(full_text_dict)
        .at(
            [
                "article",
                "front",
                "article-meta",
                "title-group",
                "article-title",
            ]
        )
        .value()
    )
    assert len(title_find) > 0
    title = title_find[0]
    if not isinstance(title, str):
        title_raw = full_text_bs.find_all("article-title")[0]
        tree = etree.fromstring(str(title_raw))
        title = str(
            etree.tostring(tree, method="text", encoding="utf-8"), "utf-8"
        )
    assert isinstance(title, str), {"title": title}

    # article version
    version_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "article-meta", "article-version"])
        .value()
    )
    assert len(version_find) > 0
    version = version_find[0]
    assert isinstance(version, str), {"version": version}

    # year_month
    year_month = find_year_month(full_text_dict)

    # publisher_id, medrxiv or biorxiv
    publish_destination = None
    publisher_id_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "journal-meta", "journal-id"])
        .value()[0]
    )
    if not isinstance(publisher_id_find, dict):
        preprint_source = (
            py_.chain(publisher_id_find)
            .filter(lambda e: e["@journal-id-type"] == "publisher-id")
            .thru(lambda col: col[0]["#text"])
            .value()
        )
        publish_destination = (
            py_.chain(publisher_id_find)
            .filter(lambda e: e["@journal-id-type"] == "destination")
            .thru(lambda col: col[0]["#text"])
            .value()
        )
    else:
        preprint_source = publisher_id_find["#text"]
    assert isinstance(preprint_source, str), {
        "preprint_source": preprint_source
    }

    # category
    category = find_category(full_text_dict)

    res = {
        "doi": doi,
        "title": title,
        "version": version,
        "category": category,
        "year_month": year_month,
        "preprint_source": preprint_source,
        "publish_destination": publish_destination,
        # NOTE: might revisit this later, after a formal round of extract
        # "publish_info": publish_info,
    }
    return res


def find_year_month(
    full_text_dict: Dict[str, Any]
) -> Optional[Dict[str, int]]:
    pub_date_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "article-meta", "history", "date"])
        .flatten()
        .filter(lambda col: col["@date-type"] == "accepted")
        .thru(lambda e: e[0] if len(e) > 0 else None)
        .value()
    )
    if pub_date_find is None:
        pub_date_find = (
            py_.chain(full_text_dict)
            .at(["article", "front", "article-meta", "history", "date"])
            .flatten()
            .filter(lambda col: col["@date-type"] == "rev-recd")
            .thru(lambda e: e[0] if len(e) > 0 else None)
            .value()
        )
        if pub_date_find is None:
            return None
    assert isinstance(pub_date_find, dict), {"pub_date_find": pub_date_find}
    year_month = {
        "year": int(pub_date_find["year"]),
        "month": int(pub_date_find["month"]),
    }
    return year_month


def find_category(full_text_dict: Dict[str, Any]) -> Optional[str]:
    category_find = (
        py_.chain(full_text_dict)
        .at(
            [
                "article",
                "front",
                "article-meta",
                "article-categories",
                "subj-group",
                "subject",
            ]
        )
        .value()[0]
    )
    if category_find is not None:
        return category_find
    else:
        category_find = (
            py_.chain(full_text_dict)
            .at(
                [
                    "article",
                    "front",
                    "article-meta",
                    "article-categories",
                    "subj-group",
                ]
            )
            .thru(lambda e: e[0])
            .filter(lambda col: col["@subj-group-type"] == "hwp-journal-coll")
            .value()
        )
        if len(category_find) > 0 and category_find[0] is not None:
            return category_find[0]["subject"]
        else:
            category_find = (
                py_.chain(full_text_dict)
                .at(
                    [
                        "article",
                        "front",
                        "article-meta",
                        "article-categories",
                        "subj-group",
                    ]
                )
                .thru(lambda e: e[0])
                .filter(lambda col: col["@subj-group-type"] == "heading")
                .value()
            )
            # Somehow biorxiv put two "heading"-s
            if len(category_find) == 2:
                return category_find[1]["subject"]
            else:
                return None
