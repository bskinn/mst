r"""*Core module for* ``mst``.

``mst`` implements tooling to scrape and store meeting info
from MS&T conferences.

**Author**
    Brian Skinn (brian.skinn@gmail.com)

**File Created**
    26 Aug 2021

**Copyright**
    \(c) Brian Skinn 2021

**Source Repository**
    https://github.com/bskinn/mst

**Documentation**
    *pending*

**License**
    The MIT License; see |license_txt|_ for full license terms

**Members**

"""


import os
import sys
from pathlib import Path

import bs4  # type: ignore
import requests as rq
import tinydb as tdb
from bs4 import BeautifulSoup as BSoup  # type: ignore

URL_ROOT: str = "http://www.programmaster.org"
URL_MST_STEM: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID="
URL_MST18: str = URL_MST_STEM + "8B0BF2B4D6505BA8852580CF005B20F8"
URL_MST19: str = URL_MST_STEM + "7E9C94165C3B857D852582340050B6D7"
URL_MST20: str = URL_MST_STEM + "EB8595226BB57C208525831F00014E65"
URL_MST21: str = URL_MST_STEM + "B6C7F14C3E2EE67A852584D3004B3D35"


KEY_SYMP_NAME: str = "symp_name"
KEY_SYMP_URL: str = "symp_url"
KEY_PREZ_NAME: str = "prez_name"
KEY_AUTHORS: str = "prez_authors"
KEY_ABSTRACT: str = "prez_abstract"
KEY_PREZ_URL: str = "prez_url"

INDEX_AUTHORS: int = 10
INDEX_ABSTRACT: int = 14


def get_symposia_anchors(*, url: str) -> list[bs4.Tag]:
    """Get the list of symposia URLs posted at the given top URL."""
    resp = rq.get(url)
    soup = BSoup(resp.text, "html.parser")

    return [
        a
        for a in soup("a")
        if (href := a["href"]).endswith(url[-8:]) and "OpenDocument" in href
    ]


def get_prez_anchors(*, url: str) -> list[bs4.Tag]:
    """Get the list of talks for a given symposium."""
    resp = rq.get(url)
    soup = BSoup(resp.text, "html.parser")

    return [a for a in soup("a") if a["href"].endswith("OpenDocument")]


def get_prez_data(*, url: str) -> dict[str, str]:
    """Scrape presentation data from a presentation page."""
    resp = rq.get(url)
    soup = BSoup(resp.text, "html.parser")

    tds = [td for td in soup("td") if td.a is None]

    return {
        KEY_AUTHORS: tds[INDEX_AUTHORS].text,
        KEY_ABSTRACT: tds[INDEX_ABSTRACT].text,
    }


def scrape_meeting(
    *, db: tdb.TinyDB, url: str, verbose: bool = False, width: int = 40
) -> None:
    """Run all the things."""
    for symp_anchor in get_symposia_anchors(url=url):
        symp_name: str = symp_anchor.text
        symp_url: str = URL_ROOT + symp_anchor["href"]

        if verbose:
            print(f"Starting '{symp_name[:width]} ...'")

        for prez_anchor in get_prez_anchors(url=symp_url):
            prez_name: str = prez_anchor.text

            if verbose:
                print(f"... Talk '{prez_name[:width]} ...'")

            prez_data = get_prez_data(url=(prez_url := URL_ROOT + prez_anchor["href"]))
            prez_data.update(
                {
                    KEY_PREZ_NAME: prez_name,
                    KEY_PREZ_URL: prez_url,
                    KEY_SYMP_NAME: symp_name,
                    KEY_SYMP_URL: symp_url,
                }
            )

            db.insert(prez_data)

        if verbose:
            print(f"Done with '{symp_name[:width]} ...'\n")


def check_data(data: dict[str, dict[str, dict[str, str]]], /) -> None:
    """Rough check to be sure data pulled ok."""
    for symp_key in data:
        for prez_key in (symp := data[symp_key]):
            if (
                len((prez := symp[prez_key])["prez_abstract"]) < 100
                or len(prez["prez_authors"]) < 10
            ):
                print(prez_key)
                print(prez["prez_authors"])
                print(prez["prez_abstract"], end="\n\n\n")


def bind_db(*, path: Path) -> tdb.TinyDB:
    """Bind a database at the given Path."""
    return tdb.TinyDB(os.fsdecode(path))


def main() -> int:
    """Run all of the things there are to run."""


if __name__ == "__main__":
    sys.exit(main())
