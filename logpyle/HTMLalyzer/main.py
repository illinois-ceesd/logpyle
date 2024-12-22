import asyncio
import base64
import json
import os
from typing import Any

import js  # type: ignore
import micropip  # type: ignore
from js import DOMParser, document
from pyodide.ffi import create_proxy  # type: ignore


class DataFile:
    def __init__(self, names: list[str]):
        self.names = names
        self.constants: dict[str, Any] = {}
        self.quantities: dict[str, dict[str, Any]] = {}


next_id = 1

# this HTML string was imported from newFile.html
file_div = """
$new_file_html
"""

logpyle_py_file = "$logpyle_py_file"
runalyzer_py_file = "$runalyzer_py_file"
runalyzer_gather_py_file = "$runalyzer_gather_py_file"


pymbolic_whl_file_str = "$pymbolic_whl_file_str"
pymbolic_whl_file_name = "$pymbolic_whl_file_name"


async def import_logpyle() -> None:
    # Currently we are expecting to have pymbolic built from source
    # to a whl file every so often. Micropip inside of pyodide can
    # only install packages that have a pure python whl file in pypi.
    # To build a new whl file, clone pymbolic and ensure that you have
    # installed the build package.
    # Inside of pymbolic, run python3 -m build --wheel
    # This will generate a directory dist which will have your wheel file.
    # Copy this into HTMLalyzer and remove the old version.

    # install pymbolic from whl file
    whl_base_64 = pymbolic_whl_file_str.encode("utf-8")
    whl_binary_data = base64.decodebytes(whl_base_64)
    with open(pymbolic_whl_file_name, "wb") as f:
        f.write(whl_binary_data)
    await micropip.install("emfs:" + pymbolic_whl_file_name)

    # install logpyle in ecmascript virtual filesystem
    os.mkdir("./logpyle")
    # copy __init__.py
    py_base_64 = logpyle_py_file.encode("utf-8")
    py_binary_data = base64.decodebytes(py_base_64)
    with open("logpyle/__init__.py", "wb") as f:
        f.write(py_binary_data)
    # copy runalyzer.py
    py_base_64 = runalyzer_py_file.encode("utf-8")
    py_binary_data = base64.decodebytes(py_base_64)
    with open("logpyle/runalyzer.py", "wb") as f:
        f.write(py_binary_data)
    # copy runalyzer_gather.py
    py_base_64 = runalyzer_gather_py_file.encode("utf-8")
    py_binary_data = base64.decodebytes(py_base_64)
    with open("logpyle/runalyzer_gather.py", "wb") as f:
        f.write(py_binary_data)


def clear_term() -> None:
    term = document.getElementById("terminal").children[0]
    term.innerHTML = ""


def add_file_func() -> None:
    global next_id

    file_list = document.getElementById("file_list")
    parser = DOMParser.new()
    html = parser.parseFromString(file_div.format(id=str(next_id)), "text/html")
    file_list.appendChild(html.body)

    new_file = document.getElementById(str(next_id))
    new_file.style.backgroundColor = "#E0E0E0"

    # attach listener to new file input
    input = document.getElementById("file" + str(next_id))
    input.addEventListener("change", create_proxy(store_file))

    next_id = next_id + 1


async def run_plot(event: Any) -> None:  # noqa: RUF029
    from logpyle.runalyzer import make_wrapped_db
    id = event.target.getAttribute("param")
    output = document.getElementById("output" + str(id))
    output.id = "graph-area"
    run_db = make_wrapped_db(file_dict[id].names, True, True)
    q1 = document.getElementById("quantity1_" + str(id)).value
    q2 = document.getElementById("quantity2_" + str(id)).value
    query = f"select ${q1}, ${q2}"
    cursor = run_db.db.execute(run_db.mangle_sql(query))
    columnnames = [column[0] for column in cursor.description]
    run_db.plot_cursor(cursor, labels=columnnames)

    output.id = "output" + str(id)


async def run_chart(event: Any) -> None:  # noqa: RUF029
    id = event.target.getAttribute("param")
    x_quantity: str = document.getElementById("quantity1_" + str(id)).value
    x = file_dict[id].quantities[x_quantity]
    x_vals = x["vals"]
    x_vals = [ele[0] for ele in x_vals]

    y_vals: dict[str, dict[str, Any]] = {}
    y_quantities_div = document.getElementById("yQuantities" + str(id))
    for y_quantity_div in y_quantities_div.children:
        y_values_elements = y_quantity_div.children
        y_name = y_values_elements[0].value
        color = y_values_elements[1].value

        y_ele = file_dict[id].quantities[y_name]

        y_val = y_ele["vals"]
        y_val = [ele[0] for ele in y_val]

        units = y_ele["units"]

        y_vals[y_name] = {}
        y_vals[y_name]["vals"] = y_val
        y_vals[y_name]["color"] = color
        y_vals[y_name]["units"] = units

    js.chartsOutputGraph(
            id,
            json.dumps(x_vals),
            json.dumps(y_vals),
            )


async def add_table_list(event: Any) -> None:  # noqa: RUF029
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
    del_button.addEventListener("click", create_proxy(remove_table_ele))
    item.appendChild(text)
    item.appendChild(del_button)
    table_list.appendChild(item)


async def add_line(event: Any) -> None:  # noqa: RUF029
    id = event.target.getAttribute("param1")
    i = event.target.param2
    event.target.param2 = i + 1
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


async def remove_table_ele(event: Any) -> None:  # noqa: RUF029
    event.target.parentElement.remove()


def download_table(event: Any) -> None:
    id = event.target.getAttribute("param")

    names = []
    table_list = document.getElementById("tableList" + str(id))
    for li in table_list.children:
        names.append(li.children[0].innerHTML)
    if len(names) == 0:
        print("no quantities in table list")
        return

    quantities = {}
    for name in names:
        vals = file_dict[id].quantities[name]["vals"]
        vals = [ele[0] for ele in vals]
        quantities[name] = vals

    title = "# " + " vs. ".join(quantities.keys())

    body = ""
    items = list(quantities.values())
    for line_num in range(len(items[0])):
        cur_vals = [str(ele[line_num]) for ele in items]
        line = "\t".join(cur_vals) + "\n"
        body += line

    js.download("output.txt", title + "\n" + body)


def print_table(event: Any) -> None:
    from logpyle.runalyzer import make_wrapped_db
    id = event.target.getAttribute("param")

    names = []
    table_list = document.getElementById("tableList" + str(id))
    for li in table_list.children:
        names.append(li.children[0].innerHTML)
    if len(names) == 0:
        print("no quantities in table list")
        return

    names = ["$" + s for s in names]
    query_args = ", ".join(names)
    # should remake in the future to store the connection instead of
    # re-gathering it every time the button is pressed
    run_db = make_wrapped_db(file_dict[id].names, True, True)
    run_db.print_cursor(run_db.q("select " + query_args))


async def store_file(event: Any) -> None:
    global file_dict
    file_list = event.target.files.to_py()
    from js import Uint8Array, document

    from logpyle.runalyzer import make_wrapped_db
    id = event.target.parentElement.parentElement.id

    # write database file
    for f1 in file_list:
        with open(f1.name, "wb") as file:
            data = Uint8Array.new(await f1.arrayBuffer())
            file.write(bytearray(data))

    names = [f1.name for f1 in file_list]
    file_dict[id] = DataFile(names)

    # extract constants from sqlite file
    run_db = make_wrapped_db(file_dict[id].names, True, True)
    cursor = run_db.db.execute("select * from runs")
    columns = [col[0] for col in cursor.description]
    vals = list(next(iter(cursor)))
    for (col, val) in zip(columns, vals, strict=False):
        file_dict[id].constants[col] = val

    # extract quantities from sqlite file
    cursor = run_db.db.execute("select * from quantities order by name")
    columns = [col[0] for col in cursor.description]
    for row in cursor:
        q_id, q_name, q_unit, q_desc, q_rank_agg = row
        tmp_cur = run_db.db.execute(run_db.mangle_sql(
            f"select ${q_name}"))

        vals = list(tmp_cur)
        file_dict[id].quantities[q_name] = {"vals": vals, "id":  q_id,
                                            "units": q_unit, "desc": q_desc,
                                            "rank_agg":  q_rank_agg}

    # display constants
    constants_table = document.getElementById("constants_table" + str(id))
    for key, value in file_dict[id].constants.items():
        row = document.createElement("tr")
        row.className = "constantsTr"

        quantity_ele = document.createElement("td")
        quantity_ele.className = "constantsTd"
        quantity_ele.innerHTML = key
        row.appendChild(quantity_ele)

        units_ele = document.createElement("td")
        units_ele.className = "constantsTd"
        units_ele.innerHTML = value
        row.appendChild(units_ele)

        # append the row to the body of the table
        constants_table.children[1].appendChild(row)

    # display quantities
    quantities_table = document.getElementById("quantities_table" + str(id))
    for q_name, quantity in file_dict[id].quantities.items():
        row = document.createElement("tr")
        row.className = "quantitiesTr"

        quantity_ele = document.createElement("td")
        quantity_ele.className = "quantitiesTd"
        quantity_ele.innerHTML = q_name
        row.appendChild(quantity_ele)

        units_ele = document.createElement("td")
        units_ele.className = "quantitiesTd"
        units_ele.innerHTML = quantity["units"]
        row.appendChild(units_ele)

        desc_ele = document.createElement("td")
        desc_ele .className = "quantitiesTd"
        desc_ele.innerHTML = quantity["desc"]
        row.appendChild(desc_ele)

        id_ele = document.createElement("td")
        id_ele.className = "quantitiesTd"
        id_ele.innerHTML = quantity["id"]
        row.appendChild(id_ele)

        rank_agg_ele = document.createElement("td")
        rank_agg_ele.className = "quantitiesTd"
        rank_agg_ele.innerHTML = quantity["rank_agg"]
        row.appendChild(rank_agg_ele)

        # append the row to the body of the table
        quantities_table.children[1].appendChild(row)

    # create plot group
    chart_button = document.getElementById("chartsButton" + str(id))
    chart_button .addEventListener("click", create_proxy(run_chart))
    # add quantities to quantity 1 dropdown
    plot_q1_select = document.getElementById("quantity1_" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        plot_q1_select.appendChild(item)
        if quantity == "step":
            plot_q1_select.value = quantity

    # add quantities to quantity 2 dropdown
    plot_q2_select = document.getElementById("quantity2_" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        plot_q2_select.appendChild(item)

    # construct plot footer
    add_line_button = document.getElementById("addLineButton" + str(id))
    add_line_button.addEventListener("click", create_proxy(add_line))
    add_line_button.param2 = 1

    # construct table header
    table_button = document.getElementById("tableButton" + str(id))
    table_button.addEventListener("click", create_proxy(add_table_list))

    # add quantities to table dropdown
    table_select = document.getElementById("tableQuantitySelect" + str(id))
    for quantity in file_dict[id].quantities:
        item = document.createElement("option")
        item.innerHTML = quantity
        item.value = quantity
        table_select.appendChild(item)

    # construct table footer
    download_table_button = document.getElementById("tableDownloadButton" + str(id))
    download_table_button.addEventListener("click",
                                           create_proxy(download_table))
    print_table_button = document.getElementById("tablePrintButton" + str(id))
    print_table_button.addEventListener("click", create_proxy(print_table))


# init file storage structure
file_dict: dict[str, Any] = {}
# ensure logpyle and dependencies are present
asyncio.ensure_future(import_logpyle())  # noqa: RUF006
# ensure that one analysis panel is present to begin with
add_file_func()
