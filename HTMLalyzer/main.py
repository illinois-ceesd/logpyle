import asyncio
import json
import js
from js import document, DOMParser
from pyscript import Element
from pyodide.ffi import create_proxy
import micropip
import base64

class dataFile:
    def __init__(self, name):
        self.name = name
        self.constants = {}
        self.quantities = {}


nextId = 1

# this HTML string was imported from newFile.html
fileDiv = """
{{ newFileHTML }}
"""

logpyleWhlFileString = """
{{ logpyleWhlFileString  }}
"""
logpyleWhlFileName = "logpyle-2023.2.3-py2.py3-none-any.whl"


pymbolicWhlFileString = """
{{ pymbolicWhlFileString  }}
"""
pymbolicWhlFileName = "pymbolic-2022.2-py3-none-any.whl"


async def importLogpyle():
    import os
    # install dependencies
    # install pymbolic
    whlBase64 = pymbolicWhlFileString.encode("utf-8")
    whl_binary_data = base64.decodebytes(whlBase64)
    with open(pymbolicWhlFileName, "wb") as f:
        f.write(whl_binary_data)
    await micropip.install("emfs:"+pymbolicWhlFileName, keep_going=True, deps=True)

    # install logpyle
    whlBase64 = logpyleWhlFileString.encode("utf-8")
    whl_binary_data = base64.decodebytes(whlBase64)
    with open(logpyleWhlFileName, "wb") as f:
        f.write(whl_binary_data)
    await micropip.install("emfs:"+logpyleWhlFileName, keep_going=True, deps=True)


def addFileFunc():
    global nextId

    fileList = document.getElementById("fileList")
    parser = DOMParser.new()
    html = parser.parseFromString(fileDiv.format(str(nextId)), 'text/html')
    fileList.appendChild(html.body)

    newFile = document.getElementById(str(nextId))
    if nextId % 2 == 0:
        # grey minus some green
        newFile.style.backgroundColor = "#B0A8B0"
    else:
        # grey minus some blue
        newFile.style.backgroundColor = "#B0B0A8"

    # attach listener to new file input
    input = document.getElementById("file" + str(nextId))
    input.addEventListener("change", create_proxy(storeFile))

    nextId = nextId + 1


async def runPlot(event):
    from logpyle.runalyzer import make_wrapped_db
    id = event.target.getAttribute("param")
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    runDb = make_wrapped_db([file_dict[id].name], True, True)
    q1 = document.getElementById("quantity1_" + str(id)).value
    q2 = document.getElementById("quantity2_" + str(id)).value
    query = "select ${}, ${}".format(q1, q2)
    cursor = runDb.db.execute(runDb.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    runDb.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)

async def runChart(event):
    id = event.target.getAttribute("param")
    x_quantity: str = document.getElementById("quantity1_" + str(id)).value
    x = file_dict[id].quantities[x_quantity]
    x_vals = x["vals"]
    x_vals = [ ele[0] for ele in x_vals]
    # x_vals = [1,2]

    y_vals = {}
    y_quantities_div = document.getElementById("yQuantities" + str(id))
    for y_quantity_div in y_quantities_div.children:
        y_values_elements = y_quantity_div.children
        y_name = y_values_elements[0].value
        color = y_values_elements[1].value

        y_ele = file_dict[id].quantities[y_name]

        y_val = y_ele["vals"]
        y_val = [ ele[0] for ele in y_val]

        units = y_ele["units"]

        y_vals[y_name] = {}
        y_vals[y_name]['vals'] = y_val
        y_vals[y_name]['color'] = color
        y_vals[y_name]['units'] = units


    # y_vals = {"lol": [0.005,0.015], "lol2": [0.002,0.007]}
    js.chartsOutputGraph(
            id,
            json.dumps(x_vals),
            json.dumps(y_vals),
            )

        # file_dict[id].quantities[q_name] = {'vals':vals, 'id': q_id,
        #                                     'units':q_unit, 'desc':q_desc,
        #                                     'rank_agg': q_rank_agg}


async def addTableList(event):
    id = event.target.getAttribute("param")
    quantity = document.getElementById("tableQuantitySelect" + str(id)).value
    table_list = document.getElementById("tableList" + str(id))
    item = document.createElement("li")
    text = document.createElement("span")
    text.innerHTML = str(quantity)
    item.setAttribute("val", str(quantity))
    item.style = "margin:2px;"
    del_button = document.createElement("button")
    del_button.style.float = "right"
    del_button.innerHTML = "delete"
    del_button.addEventListener("click", create_proxy(removeTableEle))
    item.appendChild(text)
    item.appendChild(del_button)
    table_list.appendChild(item)


async def addLine(event):
    id = event.target.getAttribute("param1")
    i = event.target.param2
    event.target.param2 = event.target.param2 + 1
    y_quantities = document.getElementById("yQuantities" + str(id))

    y_div = document.createElement("div")
    y_select = document.createElement("select")
    y_color = document.createElement("input")

    y_div.setAttribute("style", "white-space:nowrap")
    y_color.setAttribute("type", "color")


    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        y_select.appendChild(item)

    y_div.appendChild(y_select)
    y_div.appendChild(y_color)

    y_quantities.appendChild(y_div)


async def removeTableEle(event):
    event.target.parentElement.remove()


async def runTable(event):
    import matplotlib.pyplot as plt
    from logpyle.runalyzer import make_wrapped_db
    id = event.target.getAttribute("param")
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    runDb = make_wrapped_db([file_dict[id].name], True, True)
    query = "select $t_sim, $t_2step"
    cursor = runDb.db.execute(runDb.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    runDb.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)


def downloadTable(event):
    id = event.target.getAttribute("param")

    names = []
    table_list = document.getElementById("tableList" + str(id))
    for li in table_list.children:
        names.append(li.children[0].innerHTML)

    quantities = {}
    for name in names:
        vals = file_dict[id].quantities[name]["vals"]
        vals  = [ ele[0] for ele in vals ]
        quantities[name] = vals

    title = "# " + " vs. ".join(quantities.keys())

    body = ""
    items = list(quantities.values())
    for line_num in range(len(items[0])):
        cur_vals = [ str(ele[line_num]) for ele in items]
        line = "\t".join(cur_vals) + "\n"
        body += line

    js.download("output.txt", title + "\n" + body)


def printTable(event):
    pass


async def storeFile(event):
    global file_dict
    fileList = event.target.files.to_py()
    from js import document, Uint8Array
    from logpyle.runalyzer import make_wrapped_db
    id = event.target.parentElement.parentElement.id

    # write database file
    for f1 in fileList:
        with open(f1.name, 'wb') as file:
            data = Uint8Array.new(await f1.arrayBuffer())
            file.write(bytearray(data))

    for f1 in fileList:
        file_dict[id] = dataFile(f1.name)

    # extract constants from sqlite file
    runDb = make_wrapped_db([file_dict[id].name], True, True)
    cursor = runDb.db.execute("select * from runs")
    columns = [col[0] for col in cursor.description]
    vals = list([row for row in cursor][0])
    for (col, val) in zip(columns, vals):
        file_dict[id].constants[col] = val

    # extract quantities from sqlite file
    cursor = runDb.db.execute("select * from quantities order by name")
    columns = [col[0] for col in cursor.description]
    for row in cursor:
        q_id, q_name, q_unit, q_desc, q_rank_agg = row
        tmp_cur = runDb.db.execute(runDb.mangle_sql(
            "select ${}".format(q_name)))

        vals = [val for val in tmp_cur]
        file_dict[id].quantities[q_name] = {'vals':vals, 'id': q_id,
                                            'units':q_unit, 'desc':q_desc,
                                            'rank_agg': q_rank_agg}

    # display constants
    constantsTable = document.getElementById("constantsTable" + str(id))
    for key, value in file_dict[id].constants.items():
        # item = document.createElement("li")
        # item.innerHTML = str(k) + ": " + str(v)
        # constants_list.appendChild(item)

        row = document.createElement('tr')
        row.className = "constantsTr"

        quantity_ele = document.createElement('td')
        quantity_ele.className = "constantsTd"
        quantity_ele.innerHTML = key
        row.appendChild(quantity_ele)

        units_ele = document.createElement('td')
        units_ele.className = "constantsTd"
        units_ele.innerHTML = value
        row.appendChild(units_ele)

        # append the row to the body of the table
        constantsTable.children[1].appendChild(row)

    # display quantities
    quantitiesTable = document.getElementById("quantitiesTable" + str(id))
    for q_name, quantity in file_dict[id].quantities.items():
        row = document.createElement('tr')
        row.className = "quantitiesTr"

        quantity_ele = document.createElement('td')
        quantity_ele.className = "quantitiesTd"
        quantity_ele.innerHTML = q_name
        row.appendChild(quantity_ele)

        units_ele = document.createElement('td')
        units_ele.className = "quantitiesTd"
        units_ele.innerHTML = quantity['units']
        row.appendChild(units_ele)

        desc_ele = document.createElement('td')
        desc_ele .className = "quantitiesTd"
        desc_ele.innerHTML = quantity['desc']
        row.appendChild(desc_ele)

        id_ele = document.createElement('td')
        id_ele.className = "quantitiesTd"
        id_ele.innerHTML = quantity['id']
        row.appendChild(id_ele)

        rank_agg_ele = document.createElement('td')
        rank_agg_ele.className = "quantitiesTd"
        rank_agg_ele.innerHTML = quantity['rank_agg']
        row.appendChild(rank_agg_ele)

        # append the row to the body of the table
        quantitiesTable.children[1].appendChild(row)


    # create plot group
    chart_button = document.getElementById("chartsButton" + str(id))
    chart_button .addEventListener("click", create_proxy(runChart))
    # add quantites to quantity 1 dropdown
    plot_q1_select = document.getElementById("quantity1_" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        plot_q1_select.appendChild(item)
        if quantity == "step":
            plot_q1_select.value = quantity

    # add quantites to quantity 2 dropdown
    plot_q2_select = document.getElementById("quantity2_" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        plot_q2_select.appendChild(item)

    # construct plot footer
    add_line_button = document.getElementById("addLineButton" + str(id))
    add_line_button.addEventListener("click", create_proxy(addLine))
    add_line_button.param2 = 1


    # construct table header
    table_button = document.getElementById("tableButton" + str(id))
    table_button.addEventListener("click", create_proxy(addTableList))

    # add quantites to table dropdown
    table_select = document.getElementById("tableQuantitySelect" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        table_select.appendChild(item)




    # construct table footer
    download_table_button = document.getElementById("tableDownloadButton" + str(id))
    download_table_button.addEventListener("click",
                                           create_proxy(downloadTable))
    print_table_button = document.getElementById("tablePrintButton" + str(id))
    print_table_button.addEventListener("click", create_proxy(printTable))


file_dict = {}
asyncio.ensure_future(importLogpyle())
