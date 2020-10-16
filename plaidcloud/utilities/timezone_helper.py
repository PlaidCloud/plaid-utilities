#!/usr/bin/env python
# coding=utf-8

"""Help with the timezone mess"""

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2013, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Proprietary'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

##################################
# Add Public Methods Below ####
##################################

TZ_ABBR = {}
TZ_NAME = {}


def tzabbr_register(abbr, long_name, region, tz_name, dst=False):
    """Registers a timezone for later use

    Args:
        abbr (str): The abbreviation for the timezone
        long_name (str): The full name of the timezone
        region (str): What region uses this timezone
        tz_name (str): The 'friendly' name of this timezone.
            Should be shorter than `long_name`
        dst (bool, optional): Should this timezone use Daylight Savings Time
    """
    TZ_ABBR[abbr] = (tz_name, dst, long_name, region)
    TZ_NAME[tz_name] = (abbr, dst, long_name, region)


def get_by_abbr(abbr):
    """Retrieves a timezone by its abbreviation

    Args:
        abbr (str): The abbreviation of the timezone
            to retrieve

    Returns:
        tuple: The timezone representation in (name, dst, long_name, region) format
    """
    try:
        return TZ_ABBR[abbr]
    except:
        return None


def get_by_name(tz_name):
    """Retrieves a timezone by its name

    Args:
        abbr (str): The name of the timezone to retrieve

    Returns:
        tuple: The timezone representation in (abbreviation, dst, long_name, region) format
    """
    try:
        return TZ_NAME[tz_name]
    except:
        return None


# Standard timezones to register
tzabbr_register("A", u"Alpha Time Zone", u"Military", "Etc/GMT-1", False)
tzabbr_register("ACDT", u"Australian Central Daylight Time", u"Australia",
                "Australia/Adelaide", True)
tzabbr_register("ACST", u"Australian Central Standard Time", u"Australia",
                "Australia/Adelaide", False)
tzabbr_register("ADT", u"Atlantic Daylight Time", u"North America",
                "America/Halifax", True)
tzabbr_register("AEDT", u"Australian Eastern Daylight Time", u"Australia",
                "Australia/Sydney", True)
tzabbr_register("AEST", u"Australian Eastern Standard Time", u"Australia",
                "Australia/Sydney", False)
tzabbr_register("AKDT", u"Alaska Daylight Time", u"North America",
                "US/Alaska", True)
tzabbr_register("AKST", u"Alaska Standard Time", u"North America",
                "US/Alaska", False)
tzabbr_register("AST", u"Atlantic Standard Time", u"North America",
                "America/Halifax", False)
tzabbr_register("AWDT", u"Australian Western Daylight Time", u"Australia",
                "Australia/West", True)
tzabbr_register("AWST", u"Australian Western Standard Time", u"Australia",
                "Australia/West", False)
tzabbr_register("B", u"Bravo Time Zone", u"Military", "Etc/GMT-2", False)
tzabbr_register("BST", u"British Summer Time", u"Europe", "Europe/London", True)
tzabbr_register("C", u"Charlie Time Zone", u"Military", "Etc/GMT-2", False)
tzabbr_register("CDT", u"Central Daylight Time", u"North America",
                "US/Central", True)
tzabbr_register("CEDT", u"Central European Daylight Time", u"Europe",
                "Etc/GMT+2", True)
tzabbr_register("CEST", u"Central European Summer Time", u"Europe",
                "Etc/GMT+2", True)
tzabbr_register("CET", u"Central European Time", u"Europe", "Etc/GMT+1", False)
tzabbr_register("CST", u"Central Standard Time", u"North America",
                "US/Central", False)
tzabbr_register("CXT", u"Christmas Island Time", u"Australia",
                "Indian/Christmas", False)
tzabbr_register("D", u"Delta Time Zone", u"Military", "Etc/GMT-2", False)
tzabbr_register("E", u"Echo Time Zone", u"Military", "Etc/GMT-2", False)
tzabbr_register("EDT", u"Eastern Daylight Time", u"North America",
                "US/Eastern", True)
tzabbr_register("EEDT", u"Eastern European Daylight Time", u"Europe",
                "Etc/GMT+3", True)
tzabbr_register("EEST", u"Eastern European Summer Time", u"Europe",
                "Etc/GMT+3", True)
tzabbr_register("EET", u"Eastern European Time", u"Europe", "Etc/GMT+2", False)
tzabbr_register("EST", u"Eastern Standard Time", u"North America",
                "US/Eastern", False)
tzabbr_register("F", u"Foxtrot Time Zone", u"Military", "Etc/GMT-6", False)
tzabbr_register("G", u"Golf Time Zone", u"Military", "Etc/GMT-7", False)
tzabbr_register("GMT", u"Greenwich Mean Time", u"Europe", "UTC", False)
tzabbr_register("H", u"Hotel Time Zone", u"Military", "Etc/GMT-8", False)
tzabbr_register("HAA", u"Heure Avancée de l'Atlantique", u"North America", u"UTC - 3 hours")
tzabbr_register("HAC", u"Heure Avancée du Centre", u"North America", u"UTC - 5 hours")
tzabbr_register("HADT", u"Hawaii-Aleutian Daylight Time", u"North America",
                "Pacific/Honolulu", True)
tzabbr_register("HAE", u"Heure Avancée de l'Est", u"North America", u"UTC - 4 hours")
tzabbr_register("HAP", u"Heure Avancée du Pacifique", u"North America", u"UTC - 7 hours")
tzabbr_register("HAR", u"Heure Avancée des Rocheuses", u"North America", u"UTC - 6 hours")
tzabbr_register("HAST", u"Hawaii-Aleutian Standard Time", u"North America",
                "Pacific/Honolulu", False)
tzabbr_register("HAT", u"Heure Avancée de Terre-Neuve", u"North America", u"UTC - 2:30 hours")
tzabbr_register("HAY", u"Heure Avancée du Yukon", u"North America", u"UTC - 8 hours")
tzabbr_register("HDT", u"Hawaii Daylight Time", u"North America",
                "Pacific/Honolulu", True)
tzabbr_register("HNA", u"Heure Normale de l'Atlantique", u"North America", u"UTC - 4 hours")
tzabbr_register("HNC", u"Heure Normale du Centre", u"North America", u"UTC - 6 hours")
tzabbr_register("HNE", u"Heure Normale de l'Est", u"North America", u"UTC - 5 hours")
tzabbr_register("HNP", u"Heure Normale du Pacifique", u"North America", u"UTC - 8 hours")
tzabbr_register("HNR", u"Heure Normale des Rocheuses", u"North America", u"UTC - 7 hours")
tzabbr_register("HNT", u"Heure Normale de Terre-Neuve", u"North America", u"UTC - 3:30 hours")
tzabbr_register("HNY", u"Heure Normale du Yukon", u"North America", u"UTC - 9 hours")
tzabbr_register("HST", u"Hawaii Standard Time", u"North America",
                "Pacific/Honolulu", False)
tzabbr_register("I", u"India Time Zone", u"Military", "Etc/GMT-9", False)
tzabbr_register("IST", u"Irish Summer Time", u"Europe", "Europe/Dublin", True)
tzabbr_register("K", u"Kilo Time Zone", u"Military", "Etc/GMT-10", False)
tzabbr_register("L", u"Lima Time Zone", u"Military", "Etc/GMT-11", False)
tzabbr_register("M", u"Mike Time Zone", u"Military", "Etc/GMT-12", False)
tzabbr_register("MDT", u"Mountain Daylight Time", u"North America",
                "US/Mountain", True)
tzabbr_register("MESZ", u"Mitteleuroäische Sommerzeit", u"Europe", u"UTC + 2 hours")
tzabbr_register("MEZ", u"Mitteleuropäische Zeit", u"Europe", u"UTC + 1 hour")
tzabbr_register("MSD", u"Moscow Daylight Time", u"Europe",
                "Europe/Moscow", True)
tzabbr_register("MSK", u"Moscow Standard Time", u"Europe",
                "Europe/Moscow", False)
tzabbr_register("MST", u"Mountain Standard Time", u"North America",
                "US/Mountain", False)
tzabbr_register("N", u"November Time Zone", u"Military", "Etc/GMT+1", False)
tzabbr_register("NDT", u"Newfoundland Daylight Time", u"North America",
                "America/St_Johns", True)
tzabbr_register("NFT", u"Norfolk (Island) Time", u"Australia",
                "Pacific/Norfolk", False)
tzabbr_register("NST", u"Newfoundland Standard Time", u"North America",
                "America/St_Johns", False)
tzabbr_register("O", u"Oscar Time Zone", u"Military", "Etc/GMT+2", False)
tzabbr_register("P", u"Papa Time Zone", u"Military", "Etc/GMT+3", False)
tzabbr_register("PDT", u"Pacific Daylight Time", u"North America",
                "US/Pacific", True)
tzabbr_register("PST", u"Pacific Standard Time", u"North America",
                "US/Pacific", False)
tzabbr_register("Q", u"Quebec Time Zone", u"Military", "Etc/GMT+4", False)
tzabbr_register("R", u"Romeo Time Zone", u"Military", "Etc/GMT+5", False)
tzabbr_register("S", u"Sierra Time Zone", u"Military", "Etc/GMT+6", False)
tzabbr_register("T", u"Tango Time Zone", u"Military", "Etc/GMT+7", False)
tzabbr_register("U", u"Uniform Time Zone", u"Military", "Etc/GMT+8", False)
tzabbr_register("UTC", u"Coordinated Universal Time", u"Europe",
                "UTC", False)
tzabbr_register("V", u"Victor Time Zone", u"Military", "Etc/GMT+9", False)
tzabbr_register("W", u"Whiskey Time Zone", u"Military", "Etc/GMT+10", False)
tzabbr_register("WDT", u"Western Daylight Time", u"Australia",
                "Australia/West", True)
tzabbr_register("WEDT", u"Western European Daylight Time", u"Europe",
                "Etc/GMT+1", True)
tzabbr_register("WEST", u"Western European Summer Time", u"Europe",
                "Etc/GMT+1", True)
tzabbr_register("WET", u"Western European Time", u"Europe", "UTC", False)
tzabbr_register("WST", u"Western Standard Time", u"Australia",
                "Australia/West", False)
tzabbr_register("X", u"X-ray Time Zone", u"Military", "Etc/GMT+11", False)
tzabbr_register("Y", u"Yankee Time Zone", u"Military", "Etc/GMT+12", False)
tzabbr_register("Z", u"Zulu Time Zone", u"Military", "UTC", False)
