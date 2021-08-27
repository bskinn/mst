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
from typing import Optional

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
URL_MST16: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20160814183547", UID="F9FD0D2AAFA2D29285257D86004BE7A3"
)
URL_MST15: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20160708165251", UID="7E30D25D80DD485185257C85007C2E8E"
)
URL_MST14: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20150921223532", UID="5E906689DEA330B085257A8E0081D4AD"
)
URL_MST13: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20141129103957", UID="1C64FCABE1A1E644852576C30062C9E9"
)
URL_MST12: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20160507162847", UID="D62B4F5E1BB84ABD8525766B00795513"
)
URL_MST11: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20111023100917", UID="F09580088B6AEAB08525766B00710675"
)
URL_MST10: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20101124020641", UID="81D97FC62FD0900C8525750C007BAD5B"
)
URL_MST09: str = URL_ARCHIVE_TEMPLATE.format(
    tstamp="20100510071628", UID="460830349F34625F8525746B006198F0"
)


KEY_SYMP_NAME: str = "symp_name"
KEY_SYMP_URL: str = "symp_url"
KEY_PREZ_NAME: str = "prez_name"
KEY_AUTHORS: str = "prez_authors"
KEY_ABSTRACT: str = "prez_abstract"
KEY_PREZ_URL: str = "prez_url"

INDEX_AUTHORS: int = 10
INDEX_ABSTRACT: int = 14

TABLE_SYMP_URLS: str = "symp_urls"
TABLE_PREZ_URLS: str = "prez_urls"
TABLE_DATA: str = "_default"

p_root = re.compile("(https?://[^/]+)/", re.I)


class MSTError(Exception):
    """Parent class of all `mst` errors."""


class RootURLExtractError(MSTError):
    """If root URL extraction fails."""


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


def get_url_root(url: str, /) -> str:
    """Extract the root URL from the given URL."""
    mch = p_root.match(url)

    if mch is None:
        raise RootURLExtractError(url)

    return mch.group(1)


def scrape_meeting(
    *, db: tdb.TinyDB, url: str, verbose: bool = True, width: int = 40
) -> None:
    """Scrape an entire meeting, including talk info."""
    url_root = get_url_root(url)

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
                prez_data = get_prez_data(
                    url=(prez_url := url_root + prez_anchor["href"])
                )
            except Exception:  # noqa: PIE786
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


def scrape_symposia(
    *, db: tdb.TinyDB, url: str, verbose: bool = True, width: int = 40
) -> None:
    """Scrape just the symposium list and URLs."""
    url_root = get_url_root(url)

    table = db.table(TABLE_SYMP_URLS)

    for symp_anchor in get_symposia_anchors(url=url):
        symp_name: str = symp_anchor.text
        symp_url: str = url_root + symp_anchor["href"]

        if verbose:
            print(f"Found '{symp_name[:width]} ...'")

        table.insert({KEY_SYMP_NAME: symp_name, KEY_SYMP_URL: symp_url})


def scrape_symposia_and_talks(
    *, db: tdb.TinyDB, url: str, verbose: bool = True, width: int = 40
) -> None:
    """Scrape just the symposium and talk links.

    Skipping the talk info retrieval to see if it avoids the thing hanging.

    Not really. Still hangs on attempt to retrieve talk URLs, sometimes.

    """
    url_root = get_url_root(url)

    for symp_anchor in get_symposia_anchors(url=url):
        symp_name: str = symp_anchor.text
        symp_url: str = url_root + symp_anchor["href"]

        if verbose:
            print(f"Starting '{symp_name[:width]} ...'")

        for prez_anchor in get_prez_anchors(url=symp_url):
            prez_name: str = prez_anchor.text
            prez_url: str = url_root + prez_anchor["href"]

            if verbose:
                print(f"... Talk '{prez_name[:width]} ...'")

            prez_data = {
                KEY_PREZ_NAME: prez_name,
                KEY_PREZ_URL: prez_url,
                KEY_SYMP_NAME: symp_name,
                KEY_SYMP_URL: symp_url,
            }

            db.insert(prez_data)

        if verbose:
            print(f"Done with '{symp_name[:width]} ...'\n")


def retrieve_talks(
    *,
    db: tdb.TinyDB,
    verbose: bool = True,
    width: int = 40,
    skips: Optional[list[str]] = None,
) -> None:
    """Work to scrape talk details from any symposium without them."""
    skips = skips or []

    symp_table = db.table(TABLE_SYMP_URLS)
    prez_table = db.table(TABLE_PREZ_URLS)

    for symp_info in symp_table.search(tdb.Query().symp_name.search(".")):
        symp_name: str = symp_info[KEY_SYMP_NAME]
        symp_url: str = symp_info[KEY_SYMP_URL]

        skip_symp = False
        for skip in skips:
            if skip in symp_name:
                if verbose:
                    print(f"Skipping '{symp_name[:width]} ...', marked as skip.")
                skip_symp = True

        if skip_symp:
            continue

        if prez_table.search(tdb.Query().symp_name == symp_name):
            if verbose:
                print(f"Skipping '{symp_name[:width]} ...', already present.")
            continue

        if verbose:
            print(f"Starting '{symp_name[:width]} ...'")

        url_root = get_url_root(symp_url)

        for prez_anchor in get_prez_anchors(url=symp_url):
            prez_name: str = prez_anchor.text
            prez_url: str = url_root + prez_anchor["href"]

            if verbose:
                print(f"... Talk '{prez_name[:width]} ...'")

            prez_data = {
                KEY_PREZ_NAME: prez_name,
                KEY_PREZ_URL: prez_url,
                KEY_SYMP_NAME: symp_name,
                KEY_SYMP_URL: symp_url,
            }

            prez_table.insert(prez_data)

        if verbose:
            print(f"Done with '{symp_name[:width]} ...'\n")


def retrieve_talk_details(
    *,
    db: tdb.TinyDB,
    verbose: bool = True,
    width: int = 40,
    url_skips: Optional[list[str]] = None,
    name_skips: Optional[list[str]] = None,
) -> None:
    """Retrieve talk info for any talks without it."""
    url_skips = url_skips or []
    name_skips = name_skips or []

    prez_table = db.table(TABLE_PREZ_URLS)
    data_table = db.table(TABLE_DATA)

    for prez_data in prez_table.search(tdb.Query().symp_name.search(".")):
        skip_prez = False

        for skip in url_skips:
            if skip in prez_data[KEY_PREZ_URL]:
                if verbose:
                    print(
                        f"Skipping '{prez_data[KEY_PREZ_URL][:width]} ...', "
                        "marked as URL skip."
                    )
                skip_prez = True

        for skip in name_skips:
            if skip in prez_data[KEY_PREZ_NAME]:
                if verbose:
                    print(
                        f"Skipping '{prez_data[KEY_PREZ_NAME][:width]} ...', "
                        "marked as name skip."
                    )
                skip_prez = True

        if skip_prez:
            continue

        if data_table.search(
            (tdb.Query().prez_name == prez_data[KEY_PREZ_NAME])
            & (tdb.Query().symp_name == prez_data[KEY_SYMP_NAME])
        ):
            if verbose:
                print(
                    f"Skipping '{prez_data[KEY_PREZ_NAME][:width]} ...', "
                    "already present."
                )
            continue

        if verbose:
            print(f"Processing '{prez_data[KEY_PREZ_NAME][:width]}' ...", end="")

        try:
            prez_data.update(get_prez_data(url=prez_data[KEY_PREZ_URL]))
            if verbose:
                print("OK.")
        except Exception:  # noqa: PIE786
            # Just skip talks that pose problems
            prez_data.update({KEY_AUTHORS: "N/A", KEY_ABSTRACT: "N/A"})
            if verbose:
                print("Errored, storing 'N/A'.")

        data_table.insert(prez_data)


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
