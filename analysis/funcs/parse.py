from pathlib import Path
from typing import Any, Dict
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
        "title": parse_res["title"],
        "version": parse_res["version"],
        "category": parse_res["category"],
        "year_month": parse_res["year_month"],
    }
    return res


def parse_full_text(
    full_text_dict: Dict[str, Any], full_text_bs: bs, verbose: bool = False
) -> Dict[str, Any]:
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
        title = str(etree.tostring(tree, method="text", encoding="utf-8"))
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

    # category
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
        .value()
    )
    # NOTE: category could be None in rare cases
    # assert len(category_find) > 0
    category = category_find[0]
    # assert isinstance(category, str), {"category": category}

    # year_month
    accepted_date_find = (
        py_.chain(full_text_dict)
        .at(["article", "front", "article-meta", "history", "date"])
        .flatten()
        .filter(lambda col: col["@date-type"] == "accepted")
        .value()[0]
    )
    year_month = {
        "year": int(accepted_date_find["year"]),
        "month": int(accepted_date_find["month"]),
    }

    res = {
        "title": title,
        "version": version,
        "category": category,
        "year_month": year_month,
    }
    return res
