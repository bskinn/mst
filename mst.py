import json
import sys
from pathlib import Path
from typing import Union

import bs4  # type: ignore
import requests as rq
from bs4 import BeautifulSoup as BSoup  # type: ignore

URL_ROOT: str = "http://www.programmaster.org"
URL_MST18: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID=8B0BF2B4D6505BA8852580CF005B20F8"
URL_MST19: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID=7E9C94165C3B857D852582340050B6D7"
URL_MST20: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID=EB8595226BB57C208525831F00014E65"
URL_MST21: str = "http://www.programmaster.org/PM/PM.nsf/Home?OpenForm&ParentUNID=B6C7F14C3E2EE67A852584D3004B3D35"


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
    *, url: str, verbose: bool = False, width: int = 40
) -> dict[str, dict[str, dict[str, str]]]:
    """Run all the things."""
    meeting_data = {}

    for symp_anchor in get_symposia_anchors(url=url):
        symp_name: str = symp_anchor.text
        symp_url: str = URL_ROOT + symp_anchor["href"]

        symp_data = {}

        if verbose:
            print(f"Starting '{symp_name[:width]} ...'")

        for prez_anchor in get_prez_anchors(url=symp_url):
            prez_name: str = prez_anchor.text

            if verbose:
                print(f"... Talk '{prez_name[:width]} ...'")

            prez_data = get_prez_data(url=(prez_url := URL_ROOT + prez_anchor["href"]))
            prez_data.update({KEY_PREZ_URL: prez_url})

            symp_data.update({prez_name: prez_data})

        if verbose:
            print(f"Done with '{symp_name[:width]} ...'\n")

        meeting_data.update({symp_name: symp_data})

    return meeting_data


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


def main() -> int:
    """Run all of the things there are to run."""


if __name__ == "__main__":
    sys.exit(main())
