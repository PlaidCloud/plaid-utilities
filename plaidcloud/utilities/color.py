#!/usr/bin/env python
# coding=utf-8

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2009-2019, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"

friendly_names = {
    "aliceblue": "#F0F8FF","antiquewhite": "#FAEBD7","aqua": "#00FFFF","aquamarine": "#7FFFD4","azure": "#F0FFFF",
    "beige": "#F5F5DC","bisque": "#FFE4C4","black": "#000000","blanchedalmond": "#FFEBCD","blue": "#0000FF",
    "bluevoilet": "#8A2BE2","brown": "#A52A2A","burlywood": "#DEB887","cadetblue": "#5F9EA0","chartreuse": "#7FFF00",
    "chocolate": "#D2691E","coral": "#FF7F50","cornflowerblue": "#6495ED","cornsilk": "#FFF8DC","crimson": "#DC143C",
    "cyan": "#00FFFF","darkblue": "#00008B","darkcyan": "#008B8B","darkgoldenrod": "#B8860B","darkgray": "#A9A9A9",
    "darkgreen": "#006400","darkkhaki": "#BDB76B","darkmagenta": "#8B008B","darkolivegreen": "#556B2F",
    "darkorange": "#FF8C00","darkorchid": "#9932CC","darkred": "#8B0000","darksalmon": "#E9967A","darkseagreen": "#8FBC8F",
    "darkslateblue": "#483D8B","darkslategray": "#2F4F4F","darkturquoise": "#00CED1","darkviolet": "#9400D3",
    "deeppink": "#FF1493","deepskyblue": "#00BFFF","dimgray": "#696969","dodgerblue": "#1E90FF","firebrick": "#B22222",
    "floralwhite": "#FFFAF0","forestgreen": "#228B22","fuchsia": "#FF00FF","gainsboro": "#DCDCDC","ghostwhite": "#F8F8FF",
    "gold": "#FFD700","goldenrod": "#DAA520","gray": "#808080","green": "#008000","greenyellow": "#ADFF2F",
    "honeydew": "#F0FFF0","hotpink": "#FF69B4","indianred": "#CD5C5C","indigo": "#4B0082","ivory": "#FFFFF0",
    "khaki": "#F0E68C","lavender": "#E6E6FA","lavenderblush": "#FFF0F5","lawngreen": "#7CFC00","lemonchiffon": "#FFFACD",
    "lightblue": "#ADD8E6","lightcoral": "#F08080","lightcyan": "#E0FFFF","lightgoldenrodyellow": "#FAFAD2",
    "lightgreen": "#90EE90","lightgrey": "#D3D3D3","lightpink": "#FFB6C1","lightsalmon": "#FFA07A",
    "lightseagreen": "#20B2AA","lightskyblue": "#87CEFA","lightslategray": "#778899","lightsteelblue": "#B0C4DE",
    "lightyellow": "#FFFFE0","lime": "#00FF00","limegreen": "#32CD32","linen": "#FAF0E6","magenta": "#FF00FF",
    "maroon": "#800000","mediumaquamarine": "#66CDAA","mediumblue": "#0000CD","mediumorchid": "#BA55D3",
    "mediumpurple": "#9370D8","mediumseagreen": "#3CB371","mediumslateblue": "#7B68EE","mediumspringgreen": "#00FA9A",
    "mediumturquoise": "#48D1CC","mediumvioletred": "#C71585","midnightblue": "#191970","mintcream": "#F5FFFA",
    "mistyrose": "#FFE4E1","moccasin": "#FFE4B5","navajowhite": "#FFDEAD","navy": "#000080","oldlace": "#FDF5E6",
    "olive": "#808000","olivedrab": "#688E23","orange": "#FFA500","orangered": "#FF4500","orchid": "#DA70D6",
    "palegoldenrod": "#EEE8AA","palegreen": "#98FB98","paleturquoise": "#AFEEEE","palevioletred": "#D87093",
    "papayawhip": "#FFEFD5","peachpuff": "#FFDAB9","peru": "#CD853F","pink": "#FFC0CB","plum": "#DDA0DD",
    "powderblue": "#B0E0E6","purple": "#800080","red": "#FF0000","rosybrown": "#BC8F8F","royalblue": "#4169E1",
    "saddlebrown": "#8B4513","salmon": "#FA8072","sandybrown": "#F4A460","seagreen": "#2E8B57","seashell": "#FFF5EE",
    "sienna": "#A0522D","silver": "#C0C0C0","skyblue": "#87CEEB","slateblue": "#6A5ACD","slategray": "#708090",
    "snow": "#FFFAFA","springgreen": "#00FF7F","steelblue": "#4682B4","tan": "#D2B48C","teal": "#008080",
    "thistle": "#D8BFD8","tomato": "#FF6347","turquoise": "#40E0D0","violet": "#EE82EE","wheat": "#F5DEB3",
    "white": "#FFFFFF","whitesmoke": "#F5F5F5","yellow": "#FFFF00","yellowgreen": "#9ACD32"
}


def colorToHex(color_input):
    """
    Convert any input case into hexdecimal format.

    Args:
        color_input (str): The color string to convert to hex

    Returns:
        str: The hex code of the input color

    Examples:
        >>> colorToHex('yellow')
        '#ffff00'
        >>> colorToHex('#0000AA')
        '#0000aa'
        >>> colorToHex('not a valid color')
        '#330000'
    """
    result = ""
    if isinstance(color_input, str):
        if color_input[0:1] == "#":
            result = color_input.lower()
        elif color_input in friendly_names:
            result = friendly_names[color_input].lower()
        else:
            result = "#330000"
        # TODO: do some validation so that somebody doesn't send "#asdkfjldsafjdsafjdsa" as a color.
        # Right now, we're trusting that only real colors will show up.

    return result


def processColor(color_input):
    """
    Convert any input case into RGB format.

    Args:
        color_input (str, tuple): The name or hex code or RGB tuple of a color

    Returns:
        tuple(float, float, float): The float representation of the RGB value of the color

    Examples:
        >>> processColor('yellow')
        (1.0, 1.0, 0.0)
        >>> processColor('#ffff00')
        (1.0, 1.0, 0.0)
        >>> processColor((255, 255, 0))
        (1.0, 1.0, 0.0)
        >>> processColor((1.0, 1.0, 0.0))
        (1.0, 1.0, 0.0)
    """
    result = ""
    if isinstance(color_input, str):
        if color_input[0:1] == "#":
            result = RGBToFloat(HTMLColorToRGB(color_input))
        if color_input.lower() in friendly_names:
            result = RGBToFloat(HTMLColorToRGB(friendly_names[color_input.lower()]))

    if isinstance(color_input, tuple):
        if isinstance(color_input[0], int) and isinstance(color_input[1], int) and isinstance(color_input[2], int):
            result = RGBToFloat(color_input)
        if (isinstance(color_input[0], float) and isinstance(color_input[1], float)
                and isinstance(color_input[2], float)):
            result = color_input

    return result


def HTMLColorToRGB(colorstring):
    """ convert #RRGGBB to an (R, G, B) tuple

    Args:
        colorstring (str): The string representing the color in `#RRGGBB` format

    Returns:
        tuple: A tuple of the color in (R, G, B) format

    Examples:
        >>> HTMLColorToRGB('ffff00')
        (255, 255, 0)
        >>> HTMLColorToRGB('ffff00xxx')
        Traceback (most recent call last):
        ...
        ValueError: input #ffff00xxx is not in #RRGGBB format
    """
    colorstring = colorstring.strip()
    if colorstring[0] == '#':
        colorstring = colorstring[1:]
    if len(colorstring) != 6:
        raise ValueError("input #%s is not in #RRGGBB format" % colorstring)
    r, g, b = colorstring[:2], colorstring[2:4], colorstring[4:]
    r, g, b = [int(n, 16) for n in (r, g, b)]
    return r, g, b


def RGBToFloat(colortuple):
    """ convert (R, G, B) to a (Float, Float, Float) tuple

    Args:
        colortuple (tuple): A tuple representing the color in (R, G, B) format

    Returns:
        tuple: A tuple representing the color in (Float, Float, Float) format

    Examples:
        >>> RGBToFloat((255, 255, 0))
        (1.0, 1.0, 0.0)
    """
    return float(colortuple[0]/255.0), float(colortuple[1]/255.0), float(colortuple[2]/255.0)


def HTMLColorToFloat(colorstring):
    """Converts a color in HTML format to Float format

    Args:
        colorstring (str): The color in HTML format

    Returns:
        float: The color in float format

    Examples:
        >>> HTMLColorToFloat('#ffff00')
        (1.0, 1.0, 0.0)
    """
    return RGBToFloat(HTMLColorToRGB(colorstring))


def rgb(colorstring):
    """
    This is just a shorthand way of specifying HTMLColorToFloat

    Args:
        colorstring (string): The HTML representation of the color

    Returns:
        float: the float representation of the color

    Examples:
        >>> rgb('#ffff00')
        (1.0, 1.0, 0.0)
    """
    return HTMLColorToFloat(colorstring)
