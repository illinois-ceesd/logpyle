import asyncio
from js import document
from pyscript import Element
from pyodide.ffi import create_proxy
import os
import sqlite3


class dataFile:
    def __init__(self, name):
        self.name = name
        self.constants = {}
        self.quantities = {}


nextId = 1

fileDiv = """
<div id="newFile{0}">
    <div id="constants{0}">
    </div>
    <div id="interactive{0}">
        <div id="menu{0}">
            <div id="input{0}">
            </div>
            <div id="plot{0}">
            </div>
            <div id="table{0}">
            </div>
        </div>
        <div id="output{0}">
        </div>
    </div>
</div>
"""


def addFileFunc():
    global nextId
    fileList = document.getElementById("fileList")
    newFile = document.createElement("div")
    newFile.id = str(nextId)
    newFile.style.display = "flex"
    newFile.style.flexDirection = "column"
    if nextId % 2 == 0:
        # silver color
        newFile.style.backgroundColor = "#C0C0C0"
    else:
        # darkgrey color
        newFile.style.backgroundColor = "#A9A9A9"
    menu = document.createElement("div")
    menu.style = "display:flex;flex-direction:column"
    menu.id = "menu" + str(nextId)
    input = document.createElement("input")
    input.type = "file"
    input.id = "file" + str(nextId)
    output = document.createElement("div")
    output.id = "output" + str(nextId)
    constants = document.createElement("div")
    constants.style = "display:flex;flex-direction:column"
    constants.id = "constants" + str(nextId)
    interactive = document.createElement("div")
    interactive.style = "display:flex;flex-direction:row"
    interactive.id = "interactive" + str(nextId)
    menu.appendChild(input)
    interactive.appendChild(menu)
    interactive.appendChild(output)
    newFile.appendChild(constants)
    newFile.appendChild(interactive)
    fileList.appendChild(newFile)

    # attach listener to new file input
    input.addEventListener("change", create_proxy(storeFile), False)

    nextId = nextId + 1


async def runPlot(event):
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


def toggle_dropdown(event):
    dropdownlist = event.target.parentElement.children[1]
    if dropdownlist.style.display == "none":
        dropdownlist.style.display = "block"
    else:
        dropdownlist.style.display = "none"


def update_dropdown(event):
    name = event.target.innerHTML
    dropdown = event.target.parentElement.parentElement.children[0]
    dropdown.children[0].innerHTML = name
    event.target.parentElement.style.display = "none"


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
        file_dict[id].quantities[q_name] = vals

    # display constants
    constants = document.getElementById("constants" + str(id))
    constants.style.padding = "5px"
    constants.innerHTML = "Constants:"
    constants_list = document.createElement("ul")
    constants_list.style.margin = "0px"
    for k, v in file_dict[id].constants.items():
        item = document.createElement("li")
        item.innerHTML = str(k) + ": " + str(v)
        constants_list.appendChild(item)
    constants.appendChild(constants_list)

    menu_div = document.getElementById("menu"+str(id))

    # create plot group
    plot_div = document.createElement("div")
    plot_div.style = "display:flex;flex-direction:row"
    plot_div.style.padding = "10px"
    plot_button = document.createElement("button")
    plot_button.addEventListener("click", create_proxy(runPlot))
    plot_button.innerHTML = "Create Plot"
    plot_button.style.height = "30px"
    plot_button.style.padding = "5px"
    plot_button.param = str(id)

    # create quantity 1 dropdown
    plot_q1 = document.createElement("div")
    plot_q1.className = "dropdown"
    plot_q1_select = document.createElement("div")
    plot_q1_select.className = "select"
    plot_q1_select.param = str(id)
    plot_q1_text = document.createElement("span")
    plot_q1_text.innerHTML = "Select Q1"
    plot_q1_text.id = "plotQ1Text" + str(id)
    plot_q1_i = document.createElement("i")
    plot_q1_i.className = "fa fa-chevron-left"
    plot_q1_select.appendChild(plot_q1_text)
    plot_q1_select.appendChild(plot_q1_i)
    plot_q1_select.addEventListener("click", create_proxy(toggle_dropdown))
    plot_q1_list = document.createElement("ul")
    plot_q1_list.className = "dropdown-menu"
    plot_q1_list.id = "dropdownlistQ1" + str(id)
    plot_q1_list.style.display = "none"
    for quantity in file_dict[id].quantities:
        item = document.createElement("li")
        item.innerHTML = quantity
        item.addEventListener("click", create_proxy(update_dropdown))
        plot_q1_list.appendChild(item)
    plot_q1.appendChild(plot_q1_select)
    plot_q1.appendChild(plot_q1_list)

    # create quantity 2 dropdown
    plot_q2 = document.createElement("div")
    plot_q2.className = "dropdown"
    plot_q2_select = document.createElement("div")
    plot_q2_select.className = "select"
    plot_q2_select.param = str(id)
    plot_q2_text = document.createElement("span")
    plot_q2_text.innerHTML = "Select Q2"
    plot_q2_text.id = "plotQ2Text" + str(id)
    plot_q2_i = document.createElement("i")
    plot_q2_i.className = "fa fa-chevron-left"
    plot_q2_select.appendChild(plot_q2_text)
    plot_q2_select.appendChild(plot_q2_i)
    plot_q2_select.addEventListener("click", create_proxy(toggle_dropdown))
    plot_q2_list = document.createElement("ul")
    plot_q2_list.className = "dropdown-menu"
    plot_q2_list.id = "dropdownlistQ2" + str(id)
    plot_q2_list.style.display = "none"
    for quantity in file_dict[id].quantities:
        item = document.createElement("li")
        item.innerHTML = quantity
        item.addEventListener("click", create_proxy(update_dropdown))
        plot_q2_list.appendChild(item)
    plot_q2.appendChild(plot_q2_select)
    plot_q2.appendChild(plot_q2_list)

    # construct the plot section of the menu
    plot_div.appendChild(plot_button)
    plot_div.appendChild(plot_q1)
    plot_div.appendChild(plot_q2)
    menu_div.appendChild(plot_div)

    # create table group
    table_div = document.createElement("div")
    table_div.style = "display:flex;flex-direction:column"
    table_div.style.padding = "10px"
    table_header = document.createElement("div")
    table_header.style = "display:flex;flex-direction:row"
    table_list_ele = document.createElement("ul")
    table_list_ele.id = "tableList" + str(id)
    table_footer = document.createElement("div")
    table_footer.style = "display:flex;flex-direction:row"

    # construct table header
    table_name = document.createElement("div")
    table_name.innerHTML = "Table"
    table_name.style.padding = "5px"
    table_add_quantity = document.createElement("div")
    table_add_quantity.className = "dropdown"
    table_select = document.createElement("div")
    table_select.className = "select"
    table_select.param = str(id)
    table_text = document.createElement("span")
    table_text.innerHTML = "Add quantity to table list"
    table_i = document.createElement("i")
    table_i.className = "fa fa-chevron-left"
    table_select.appendChild(table_text)
    table_select.appendChild(table_i)
    table_select.addEventListener("click", create_proxy(toggle_dropdown))
    table_list = document.createElement("ul")
    table_list.className = "dropdown-menu"
    table_list.id = "tableDropdownList" + str(id)
    table_list.style.display = "none"
    for quantity in file_dict[id].quantities:
        item = document.createElement("li")
        item.innerHTML = quantity
        item.addEventListener("click", create_proxy(add_from_dropdown))
        table_list.appendChild(item)

    table_add_quantity.appendChild(table_select)
    table_add_quantity.appendChild(table_list)

    table_header.appendChild(table_name)
    table_header.appendChild(table_add_quantity)

    # construct table footer
    download_table_button = document.createElement("button")
    download_table_button.addEventListener("click",
                                           create_proxy(downloadTable))
    download_table_button.innerHTML = "Download Table"
    download_table_button.style.height = "30px"
    download_table_button.style.padding = "5px"
    download_table_button.param = str(id)
    print_table_button = document.createElement("button")
    print_table_button.addEventListener("click", create_proxy(printTable))
    print_table_button.innerHTML = "Print Table"
    print_table_button.style.height = "30px"
    print_table_button.style.padding = "5px"
    print_table_button.param = str(id)
    table_footer.appendChild(download_table_button)
    table_footer.appendChild(print_table_button)

    # construct the table section of the menu
    table_div.appendChild(table_header)
    table_div.appendChild(table_list_ele)
    table_div.appendChild(table_footer)
    menu_div.appendChild(table_div)


file_dict = {}

# el = document.getElementById("fileList")
# el.innerHTML += fileDiv.format(str(5))
# print(el.innerHTML)

