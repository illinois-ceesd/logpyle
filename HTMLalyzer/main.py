import asyncio
from js import document, DOMParser
from pyscript import Element
from pyodide.ffi import create_proxy
import os
import sqlite3
import micropip

async def customImports():
    await micropip.install('logpyle', deps=False, keep_going=True)

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


def addFileFunc():
    global nextId

    fileList = document.getElementById("fileList")
    parser = DOMParser.new()
    html = parser.parseFromString(fileDiv.format(str(nextId)), 'text/html')
    fileList.appendChild(html.body)

    newFile = document.getElementById(str(nextId))
    if nextId % 2 == 0:
        # silver color
        newFile.style.backgroundColor = "#C0C0C0"
    else:
        # darkgrey color
        newFile.style.backgroundColor = "#A9A9A9"

    # attach listener to new file input
    input = document.getElementById("file" + str(nextId))
    input.addEventListener("change", create_proxy(storeFile))

    nextId = nextId + 1


async def runPlot(event):
    # TODO rework to go through all tds for its id checking if it begins with radio
    id = event.target.param
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    runDb = make_wrapped_db(file_dict[id].name, True, True)
    q1 = document.getElementById("plotQ1Text"+str(id)).innerHTML
    q2 = document.getElementById("plotQ2Text"+str(id)).innerHTML
    query = "select ${}, ${}".format(q1, q2)
    cursor = runDb.db.execute(runDb.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    runDb.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)

async def addLine(event):
    id = event.target.param1
    i = event.target.param2
    event.target.param2 = event.target.param2 + 1
    quantitiesTable = document.getElementById("quantitiesTable" + str(id))

    # add header
    header = document.createElement("td")
    header.innerHTML = "line " + str(i) + " (x,y)"
    header.className = "quantitiesTd"
    quantitiesTable.children[0].children[0].appendChild(header)

    # add body
    for row in quantitiesTable.children[1].children:
        div = document.createElement("td")
        div.className = "quantitiesTd"

        radio_x = document.createElement("input")
        radio_x.style.width = "50%"
        # radio file#, line#
        radio_x.name = "radio_f{0}_l{1}x".format(str(id),str(i))
        radio_x.type = "radio"

        radio_y = document.createElement("input")
        radio_y.style.width = "50%"
        # radio file#, line#
        radio_y.name = "radio_f{0}_l{1}y".format(str(id),str(i))
        radio_y.type = "radio"

        div.appendChild(radio_x)
        div.appendChild(radio_y)
        row.appendChild(div)

    pass


async def runTable(event):
    id = event.target.param
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    runDb = make_wrapped_db(file_dict[id].name, True, True)
    query = "select $t_sim, $t_2step"
    cursor = runDb.db.execute(runDb.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    runDb.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)


# def toggle_dropdown(event):
#     dropdownlist = event.target.parentElement.children[1]
#     if dropdownlist.style.display == "none":
#         dropdownlist.style.display = "block"
#     else:
#         dropdownlist.style.display = "none"


# def update_dropdown(event):
#     name = event.target.innerHTML
#     dropdown = event.target.parentElement.parentElement.children[0]
#     dropdown.children[0].innerHTML = name
#     event.target.parentElement.style.display = "none"


def downloadTable(event):
    pass


def printTable(event):
    pass


def add_from_dropdown(event):
    id = event.target.parentElement.parentElement.children[0].param
    name = event.target.innerHTML
    new_item = document.createElement("li")
    new_item.innerHTML = name
    item_list = document.getElementById("tableList" + str(id))
    item_list.appendChild(new_item)
    event.target.parentElement.style.display = "none"


async def storeFile(event):
    global file_dict
    fileList = event.target.files.to_py()
    from js import document, Uint8Array
    id = event.target.parentElement.parentElement.parentElement.id

    # write database file
    for f1 in fileList:
        open(f1.name, 'x')  # ensure that file has not yet been created
        with open(f1.name, 'wb') as file:
            data = Uint8Array.new(await f1.arrayBuffer())
            file.write(bytearray(data))

    for f1 in fileList:
        file_dict[id] = dataFile(f1.name)

    # extract constants from sqlite file
    runDb = make_wrapped_db(file_dict[id].name, True, True)
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
    constants_list = document.getElementById("constantsList" + str(id))
    for k, v in file_dict[id].constants.items():
        item = document.createElement("li")
        item.innerHTML = str(k) + ": " + str(v)
        constants_list.appendChild(item)

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
    plot_button = document.getElementById("plotButton" + str(id))
    plot_button.addEventListener("click", create_proxy(runPlot))
    plot_button.param = str(id)
    add_line_button = document.getElementById("addLineButton" + str(id))
    add_line_button.addEventListener("click", create_proxy(addLine))
    add_line_button.param1 = str(id)
    add_line_button.param2 = 1


    # # create quantity 1 dropdown
    # plot_q1_select = document.getElementById("plotQ1Select" + str(id))
    # plot_q1_select.addEventListener("click", create_proxy(toggle_dropdown))
    # plot_q1_list = document.getElementById("dropdownlistQ1"+str(id))
    # for quantity in file_dict[id].quantities:
    #     item = document.createElement("li")
    #     item.innerHTML = quantity
    #     item.addEventListener("click", create_proxy(update_dropdown))
    #     plot_q1_list.appendChild(item)

    # # create quantity 2 dropdown
    # plot_q2_select = document.getElementById("plotQ2Select" + str(id))
    # plot_q2_select.addEventListener("click", create_proxy(toggle_dropdown))
    # plot_q2_list = document.getElementById("dropdownlistQ2"+str(id))
    # for quantity in file_dict[id].quantities:
    #     item = document.createElement("li")
    #     item.innerHTML = quantity
    #     item.addEventListener("click", create_proxy(update_dropdown))
    #     plot_q2_list.appendChild(item)

    # construct table header
    # table_select = document.getElementById("tableSelect" + str(id))
    # table_select.param = str(id)
    # table_select.addEventListener("click", create_proxy(toggle_dropdown))
    # table_list = document.getElementById("tableDropdownList" + str(id))
    # for quantity in file_dict[id].quantities:
    #     item = document.createElement("li")
    #     item.innerHTML = quantity
    #     item.addEventListener("click", create_proxy(add_from_dropdown))
    #     table_list.appendChild(item)

    # construct table footer
    download_table_button = document.getElementById("tableDownloadButton" + str(id))
    download_table_button.addEventListener("click",
                                           create_proxy(downloadTable))
    download_table_button.param = str(id)
    print_table_button = document.getElementById("tablePrintButton" + str(id))
    print_table_button.addEventListener("click", create_proxy(printTable))
    print_table_button.param = str(id)


file_dict = {}
asyncio.ensure_future(customImports())
