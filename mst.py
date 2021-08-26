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
import re
import sys
from pathlib import Path

import bs4
import requests as rq
import tinydb as tdb
from bs4 import BeautifulSoup as BSoup
from opnieuw import retry


URL_MST_STEM: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID="
URL_MST18: str = URL_MST_STEM + "8B0BF2B4D6505BA8852580CF005B20F8"
URL_MST19: str = URL_MST_STEM + "7E9C94165C3B857D852582340050B6D7"
URL_MST20: str = URL_MST_STEM + "EB8595226BB57C208525831F00014E65"
URL_MST21: str = URL_MST_STEM + "B6C7F14C3E2EE67A852584D3004B3D35"

URL_ARCHIVE_TEMPLATE: str = (
    "https://web.archive.org/web/{tstamp}/http://"
    "www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID={UID}"
)
URL_MST17: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20180210214606", UID="AF3AC2183CA786AA85257EE6004E0E4E"
)

KEY_SYMP_NAME: str = "symp_name"
KEY_SYMP_URL: str = "symp_url"
KEY_PREZ_NAME: str = "prez_name"
KEY_AUTHORS: str = "prez_authors"
KEY_ABSTRACT: str = "prez_abstract"
KEY_PREZ_URL: str = "prez_url"

INDEX_AUTHORS: int = 10
INDEX_ABSTRACT: int = 14

p_root = re.compile("(https?://[^/]+)/", re.I)


@retry(
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=120,
    retry_on_exceptions=(rq.exceptions.RequestException),
)
def get_symposia_anchors(*, url: str) -> list[bs4.Tag]:
    """Get the list of symposia URLs posted at the given top URL."""
    resp = rq.get(url)
    soup = BSoup(resp.text, "html.parser")

    return [
        a
        for a in soup("a")
        if (href := a["href"]).endswith(url[-8:]) and "OpenDocument" in href
    ]


@retry(
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=120,
    retry_on_exceptions=(rq.exceptions.RequestException),
)
def get_prez_anchors(*, url: str) -> list[bs4.Tag]:
    """Get the list of talks for a given symposium."""
    resp = rq.get(url)
    soup = BSoup(resp.text, "html.parser")

    return [a for a in soup("a") if a["href"].endswith("OpenDocument")]


@retry(
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=120,
    retry_on_exceptions=(rq.exceptions.RequestException),
)
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
    url_root: str = p_root.match(url).group(1)

    for symp_anchor in get_symposia_anchors(url=url):
        symp_name: str = symp_anchor.text
        symp_url: str = url_root + symp_anchor["href"]

        if verbose:
            print(f"Starting '{symp_name[:width]} ...'")

        for prez_anchor in get_prez_anchors(url=symp_url):
            prez_name: str = prez_anchor.text

            if verbose:
                print(f"... Talk '{prez_name[:width]} ...'")

            try:
                prez_data = get_prez_data(url=(prez_url := url_root + prez_anchor["href"]))
            except Exception:
                # Just skip talks that pose problems
                prez_data = {KEY_AUTHORS: "N/A", KEY_ABSTRACT: "N/A"}

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


def check_data(db: tdb.TinyDB, /) -> None:
    """Rough check to be sure data pulled ok."""
    Prez = tdb.Query()  # noqa: N806

    def item_printer(item: dict[str, str], /) -> None:
        """Print the item info prettily."""
        print(item[KEY_PREZ_NAME])
        print(item[KEY_AUTHORS])
        print(item[KEY_ABSTRACT], end="\n\n")

    for item in db.search(
        (Prez.prez_abstract.matches(".{101}"))
        | (Prez.prez_authors.matches("^.{0,10}$"))
    ):
        item_printer(item)


def bind_db(path: Path, /) -> tdb.TinyDB:
    """Bind a database at the given Path."""
    return tdb.TinyDB(os.fsdecode(path))


def main() -> int:
    """Run all of the things there are to run."""


if __name__ == "__main__":
    sys.exit(main())
