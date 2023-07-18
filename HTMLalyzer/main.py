import asyncio
from js import document, DOMParser
from pyscript import Element
from pyodide.ffi import create_proxy
import os
import sqlite3
import micropip

async def customImports():
    pass

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
    # TODO rework to go through all tds for its id checking if it begins with radio
    id = event.target.getAttribute("param")
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    runDb = make_wrapped_db(file_dict[id].name, True, True)
    q1 = document.getElementById("quantity1_" + str(id)).value
    q2 = document.getElementById("quantity2_" + str(id)).value
    # q1 = document.getElementById("plotQ1Text"+str(id)).innerHTML
    # q2 = document.getElementById("plotQ2Text"+str(id)).innerHTML
    query = "select ${}, ${}".format(q1, q2)
    cursor = runDb.db.execute(runDb.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    runDb.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)

async def addTableList(event):
    id = event.target.getAttribute("param")
    quantity = document.getElementById("tableQuantitySelect" + str(id)).value
    table_list = document.getElementById("tableList" + str(id))
    item = document.createElement("li")
    item.textContent = str(quantity)
    item.val = str(quantity)
    table_list.appendChild(item)


async def addLine(event):
    id = event.target.getAttribute("param1")
    i = event.target.param2
    event.target.param2 = event.target.param2 + 1
    y_quantities = document.getElementById("yQuantities" + str(id))

    y_select = document.createElement("select")

    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        y_select .appendChild(item)

    y_quantities.appendChild(y_select)

    # # add header
    # header = document.createElement("td")
    # header.innerHTML = "line " + str(i) + " (x,y)"
    # header.className = "quantitiesTd"
    # quantitiesTable.children[0].children[0].appendChild(header)

    # # add body
    # for row in quantitiesTable.children[1].children:
    #     div = document.createElement("td")
    #     div.className = "quantitiesTd"

    #     radio_x = document.createElement("input")
    #     radio_x.style.width = "50%"
    #     # radio file#, line#
    #     radio_x.name = "radio_f{0}_l{1}x".format(str(id),str(i))
    #     radio_x.type = "radio"

    #     radio_y = document.createElement("input")
    #     radio_y.style.width = "50%"
    #     # radio file#, line#
    #     radio_y.name = "radio_f{0}_l{1}y".format(str(id),str(i))
    #     radio_y.type = "radio"

    #     div.appendChild(radio_x)
    #     div.appendChild(radio_y)
    #     row.appendChild(div)

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


def downloadTable(event):
    pass


def printTable(event):
    pass



async def storeFile(event):
    global file_dict
    fileList = event.target.files.to_py()
    from js import document, Uint8Array
    id = event.target.parentElement.parentElement.parentElement.id

    # write database file
    for f1 in fileList:
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
    plot_button = document.getElementById("plotButton" + str(id))
    plot_button.addEventListener("click", create_proxy(runPlot))

    # add quantites to quantity 1 dropdown
    plot_q1_select = document.getElementById("quantity1_" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        plot_q1_select.appendChild(item)

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
asyncio.ensure_future(customImports())
