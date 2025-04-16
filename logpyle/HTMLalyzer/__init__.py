def get_current_hash() -> str:
    import base64
    import hashlib
    import os

    import logpyle.HTMLalyzer as Html

    html_path = os.path.dirname(Html.__file__)

    # calculate hashes of files used to build html
    hashes_str = ""

    # File paths are relative to 'logpyle/logpyle'.
    files = [
            "__init__.py",
            "runalyzer.py",
            "runalyzer_gather.py",
            "HTMLalyzer/templates/index.html",
            "HTMLalyzer/templates/newFile.html",
            "HTMLalyzer/main.css",
            "HTMLalyzer/main.js",
            "HTMLalyzer/main.py",
            ]

    for file in files:
        with open(html_path + "/../" + file, "rb") as f:
            binary_data = f.read()
            m = hashlib.sha256()
            m.update(binary_data)
            hash = m.digest()
            hashes_str += file + ": " + base64.b64encode(hash).decode("utf-8") + "\n"

    return hashes_str


def build() -> None:
    import base64
    import os
    from string import Template

    import logpyle.HTMLalyzer as Html

    html_path = os.path.dirname(Html.__file__)

    print("Building HTML file for HTMLalyzer.")

    # gather whl filenames
    html_files = os.listdir(html_path)
    pymbolic_whl_file_name = None
    for s in html_files:
        if s.startswith("pymbolic"):
            pymbolic_whl_file_name = s
            pymbolic_whl_path = "HTMLalyzer/" + s
    assert pymbolic_whl_file_name, "pymbolic .whl file not found"

    # store pymbolic and source files as binary data in html file
    filenames_to_copy = [
            "__init__.py",
            "runalyzer.py",
            "runalyzer_gather.py",
            pymbolic_whl_path,
            ]
    files_dict = {}
    for name in filenames_to_copy:
        with open(html_path + "/../" + name, "rb") as f:
            binary_data = f.read()
            data = base64.b64encode(binary_data)  # insert as single line of text
            files_dict[name] = data.decode("utf-8")

    # get templating files
    with open(html_path + "/templates/index.html") as f:
        main_template = Template(f.read())
    with open(html_path + "/templates/newFile.html") as f:
        new_file_html = f.read()
    with open(html_path + "/main.py") as f:
        main_py = f.read()

    # insert main.py dependencies as strings
    main_py_template = Template(main_py)
    main_py = main_py_template.safe_substitute(
            new_file_html=new_file_html,
            pymbolic_whl_file_str=files_dict[pymbolic_whl_path],
            pymbolic_whl_file_name=pymbolic_whl_file_name,
            logpyle_py_file=files_dict["__init__.py"],
            runalyzer_py_file=files_dict["runalyzer.py"],
            runalyzer_gather_py_file=files_dict["runalyzer_gather.py"],
            )

    with open(html_path + "/main.css") as f:
        main_css = f.read()
    with open(html_path + "/main.js") as f:
        main_js = f.read()

    # create HTMLalyzer as a string
    content = main_template.safe_substitute(
            cssFile=main_css,
            pythonFile=main_py,
            jsFile=main_js,
            )

    # write html file to logpyle/HTMLalyzer/
    filename = "htmlalyzer.html"
    with open(html_path + "/" + filename, mode="w", encoding="utf-8") as message:
        message.write(content)

    # store file hashes
    with open(html_path + "/file_hashes.txt", "w") as f:
        hashes_str = get_current_hash()
        f.write(hashes_str)

    print("HTML file build successfully.")


if __name__ == "__main__":
    build()
