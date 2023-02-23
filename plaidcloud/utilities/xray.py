#!/usr/bin/env python
# coding=utf-8

"""
For development and debugging purposes
"""

__author__ = "Michael Rea"
__copyright__ = "© Copyright 2010-2011, Tartan Solutions, Inc"
__credits__ = ["Michael Rea, Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Michael Rea"
__email__ = "michael.rea@tartansolutions.com"

def Xray(input_object, id_list=[], level=1, printout=False, show_private=False, max_level=9):
    """
    Xray recursively dissects an object into its component parts.
    It is intended to be a debugging tool.

    To use Xray, just pass an object into it.

    Args:
        input_object (object): The object to xray
        id_list (:type:`list` of :type:`str`, optional): A list of object properties to filter to
        level (int, optional): The current level of recursion
        printout (bool, optional): Print this xray out? Defaults to `False`
        show_private (bool, optional): Show private items? Defaults to `False`
        max_level (int, optional): The maximum number of levels to recurse down.

    Returns:
        str: The formatted x-ray of `input_object`

    >>> test_a = ObjectA()
    >>> test_b = ObjectB()
    >>> test_a.set_b(test_b)
    >>> test_b.set_a(test_a)
    >>> xray_a = Xray(test_a)
    >>> isinstance(xray_a, str)
    True
    """
    #MWR 20101124 Next 4 lines block looping recursion
    #if id(input_object) in id_list:
    if level >= max_level:
        return ""
    else:
        id_list.append(id(input_object))

    cr = "\r\n"
    xray_results = ""
    if level==1:
        prepend = "".join([cr, (level * "    "), "Object Type: ", " ",str(type(input_object)),cr])
    else:
        prepend = ""

    x = True

    for index, item in enumerate(dir(input_object)):

        my_line = []
        lvl = (level * "    ")
        itm = str(item)
        #Just forcing some alignment here.  Won't work so well if items have more than 30 characters.
        length = max(0, 30 - len(itm))
        itm = itm + (length * " ")

        if item[2:] != "__" and item[:2] != "__":
        #if item[1:] != "_":
            private = False

        else:
            private = True

        if "__getattribute__" in dir(input_object):

            try:
                myobj = input_object.__getattribute__(item)
            except:
                pass
            else:
                if type(input_object.__getattribute__(item)) in([type("")]):
                    my_line.extend([lvl, "str       ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])
                elif "numpy" in str(type(input_object.__getattribute__(item))):
                    my_line.extend([lvl, str(type(input_object.__getattribute__(item))),"      ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)) in([type(x)]):
                    my_line.extend([lvl, "bool      ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)) in([type(1)]):
                    my_line.extend([lvl, "int       ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)) in([type(1.1)]):
                    my_line.extend([lvl, "float     ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)) in([type({})]):
                    #if id(input_object.__getattribute__(item)) not in id_list:
                    dict_description = XrayDict(input_object.__getattribute__(item), id_list, level+1, show_private)
                    my_line.extend([lvl, "dict      ", itm, "   <", str(id(itm)),">","[", str(level), "]", dict_description])

                elif type(input_object.__getattribute__(item)) in([type([])]):
                    #change list to dict so we can reuse XrayDict (rather than build XrayList)
                    my_dict = {}
                    for my_index, my_item in enumerate(input_object.__getattribute__(item)):
                        my_dict[my_index] = my_item

                    #if id(input_object.__getattribute__(item)) not in id_list:
                    dict_description = XrayDict(my_dict, id_list, level+1, show_private)
                    my_line.extend([lvl, "list      ", itm, "   <", str(id(itm)),">","[", str(level), "]", dict_description])

                elif type(input_object.__getattribute__(item)) in([type(())]):
                    my_line.extend([lvl, "tuple     ", itm, str(input_object.__getattribute__(item)), "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)) in([type(None)]):
                    my_line.extend([lvl, "None     ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

                elif type(input_object.__getattribute__(item)).__name__ == "instancemethod":
                    my_line.extend([lvl, "method    ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

                elif "builtin" in str(type(input_object.__getattribute__(item))):
                    my_line.extend([lvl, "builtin   ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

                elif "__class__" in item:
                    my_line.extend([lvl, "class     ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

                elif "method-wrapper" in str(type(input_object.__getattribute__(item))):
                    my_line.extend([lvl, "meth-wrap ", "   <", str(id(itm)),">","[", str(level), "]"])

                else:
                    #if id(input_object.__getattribute__(item)) not in id_list:
                    #my_line.extend(type(item))
                    my_line.extend([lvl, "--- object    ", itm, str(input_object.__getattribute__(item)).replace("\n", "").replace("\r", ""), "   <", str(id(itm)),">","[", str(level), "]", Xray(input_object.__getattribute__(item), id_list, level + 1, show_private)])
                    #return contents.replace("\n", "").replace("\r", "")

                if private == True and show_private == False:
                    pass
                else:
                    xray_results = "".join([xray_results, cr, " ".join(my_line)])
        else:
            my_line.extend([lvl, "---", "   <", item,">","[", str(level), "]"])
            xray_results = "".join([xray_results, cr, " ".join(my_line)])

            pass

    xray_results = "".join([prepend, xray_results])

    return xray_results

def XrayDict(input_object, id_list, level=1, printout=False, show_private = False, max_level = 9):
    """XRays a provided dict

    Args:
        input_object (dict): The dict to xray
        id_list (:type:`list` of :type:`str`, optional): A list of object properties to filter to
        level (int, optional): The current level of recursion
        printout (bool, optional): Print this xray out? Defaults to `False`
        show_private (bool, optional): Show private items? Defaults to `False`
        max_level (int, optional): The maximum number of levels to recurse down.

    Returns:
        str: The formatted x-ray of `input_object`
    """
    #MWR 20101124 Next 4 lines block looping recursion
    if level >= max_level:
        return ""
    else:
        id_list.append(id(input_object))

    xray_results = ""
    cr = "\r\n"
    if level==1:
        prepend = "".join([cr, (level * "    "), "Object Type: ", " ",str(type(input_object)),cr])
    else:
        prepend = ""

    x = True

    for key in input_object.keys():
        my_line = []
        lvl = (level * "    ")
        itm = str(key)
        length = 20 - len(itm)
        itm = itm + (length * " ")

        if str(key)[2:] != "__" and str(key)[:2] != "__":
        #if str(key)[1:] != "_":
            private = False

        else:
            private = True

        if type(input_object.get(key)) in([type("")]):
            my_line.extend([lvl, "str       ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif "numpy" in str(type(input_object.get(key))):
            my_line.extend([lvl, str(type(input_object.get(key))),"      ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)) in([type(x)]):
            my_line.extend([lvl, "bool      ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)) in([type(1)]):
            my_line.extend([lvl, "int       ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)) in([type(1.1)]):
            my_line.extend([lvl, "float     ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)) in([type({})]):
            #if id(input_object.get(key)) not in id_list:
            dict_description = XrayDict(input_object.get(key), id_list, level+1, show_private)
            my_line.extend([lvl, "dict      ", "   <", str(id(itm)),">","[", str(level), "]", itm,dict_description])

        elif type(input_object.get(key)) in([type([])]):
            #change list to dict so we can reuse XrayDict (rathery than build XrayList)
            my_dict = {}
            for my_index, my_item in enumerate(input_object.get(key)):
                my_dict[my_index] = my_item

            #if id(input_object.get(key)) not in id_list:
            dict_description = XrayDict(my_dict, id_list, level+1, show_private)
            my_line.extend([lvl, "list      ", itm, "   <", str(id(itm)),">","[", str(level), "]",dict_description])

        elif type(input_object.get(key)) in([type(())]):
            my_line.extend([lvl, "tuple     ", itm, str(input_object.get(key)), "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)) in([type(None)]):
            my_line.extend([lvl, "None      ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

        elif type(input_object.get(key)).__name__ == "instancemethod":
            my_line.extend([lvl, "method    ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

        elif "builtin" in str(type(input_object.get(key))):
            my_line.extend([lvl, "builtin   ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

        #elif "__class__" in key:
        #    #TypeError: argument of type 'VisitableType' is not iterable
        #    my_line.extend([lvl, "class     ", itm])

        elif "method-wrapper" in str(type(input_object.get(key))):
            my_line.extend([lvl, "meth-wrap ", itm, "   <", str(id(itm)),">","[", str(level), "]"])

        else:
            my_line.extend([lvl, "+++ object    ", str(type(input_object.get(key))).replace("\n", "").replace("\r", ""), "   <", str(id(itm)),">","[", str(level), "]", Xray(input_object.get(key), id_list, level + 1, show_private)])

        if private == True and show_private == False:
            pass
        else:
            xray_results = "".join([xray_results, cr, " ".join(my_line)])

    xray_results = "".join([prepend, xray_results])

    return xray_results


def __add(xray_results, new_line):
    a = len(xray_results)
    new_line.append("\r\n")
    xray_results = " ".join(new_line)

    return xray_results


class ObjectA(object):

    def __init__(self):
        pass

    def set_b(self, new_obj):
        self.b = new_obj


class ObjectB(object):

    def __init__(self):
        pass

    def set_a(self, new_obj):
        self.a = new_obj
