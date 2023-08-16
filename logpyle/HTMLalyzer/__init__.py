def setup() -> None:
    import base64
    import hashlib
    import os

    from jinja2 import Environment, FileSystemLoader, environment

    import logpyle.HTMLalyzer
    from logpyle import HTMLalyzer

    html_path = os.path.dirname(logpyle.HTMLalyzer.__file__)

    print("Building HTML file for HTMLalyzer!")

    # gather whl filenames
    html_files = os.listdir(html_path)
    pymbolic_whl_file_name = None
    for s in html_files:
        if s.startswith("pymbolic"):
            pymbolic_whl_file_name = s
    assert pymbolic_whl_file_name, "pymbolic .whl file not found"

    hashes_str = ""

    # get logpyle
    with open(html_path+"/../__init__.py", "rb") as f:
        binary_data = f.read()
        m = hashlib.sha256()
        m.update(binary_data)
        hash = m.digest()
        hashes_str += "logpyle:" + base64.b64encode(hash).decode("utf-8") + "\n"
        data = base64.b64encode(binary_data)
        logpyle_py_file = data.decode("utf-8")

    # get runalyzer
    with open(html_path+"/../runalyzer.py", "rb") as f:
        binary_data = f.read()
        m = hashlib.sha256()
        m.update(binary_data)
        hash = m.digest()
        hashes_str += "runalyzer:" + base64.b64encode(hash).decode("utf-8") + "\n"
        data = base64.b64encode(binary_data)
        runalyzer_py_file = data.decode("utf-8")

    # get runalyzer_gather
    with open(html_path+"/../runalyzer_gather.py", "rb") as f:
        binary_data = f.read()
        m = hashlib.sha256()
        m.update(binary_data)
        hash = m.digest()
        hashes_str += "runalyzer_gather:" + base64.b64encode(hash).decode("utf-8") + "\n"
        data = base64.b64encode(binary_data)
        runalyzer_gather_py_file = data.decode("utf-8")

    # get version.py
    with open(html_path+"/../version.py", "rb") as f:
        binary_data = f.read()
        m = hashlib.sha256()
        m.update(binary_data)
        hash = m.digest()
        hashes_str += "version:" + base64.b64encode(hash).decode("utf-8") + "\n"
        data = base64.b64encode(binary_data)
        version_py_file = data.decode("utf-8")

    # get pymbolic whl
    with open(html_path+"/"+pymbolic_whl_file_name, "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        pymbolic_whl_file_str = data.decode("utf-8")

    # store file hashes
    with open(html_path+"/file_hashes.txt", "w") as f:
        f.write(hashes_str)

    html_path = os.path.dirname(HTMLalyzer.__file__)
    enviroment = Environment(loader=FileSystemLoader(html_path+"/templates/"))
    template = enviroment.get_template("index.html")


    new_file_html = open(html_path+"/templates/newFile.html", "r").read()
    main_py = open(html_path+"/main.py", "r").read()
    main_py_env: environment.Template = Environment().from_string(main_py)

    # insert main.py dependencies as strings
    main_py = main_py_env.render(
            new_file_html=new_file_html,
            pymbolic_whl_file_str=pymbolic_whl_file_str,
            pymbolic_whl_file_name=pymbolic_whl_file_name,
            logpyle_py_file=logpyle_py_file,
            runalyzer_py_file=runalyzer_py_file,
            runalyzer_gather_py_file=runalyzer_gather_py_file,
            version_py_file=version_py_file,
            )
    main_css = open(html_path+"/main.css", "r").read()
    main_js = open(html_path+"/main.js", "r").read()

    # create HTMLalyzer as a string
    content = template.render(
            cssFile=main_css,
            pythonFile=main_py,
            jsFile=main_js,
            )

    # write html file to logpyle/HTMLalyzer/
    filename = "web-interface.html"
    with open(html_path+"/"+filename, mode="w", encoding="utf-8") as message:
        message.write(content)

    print("HTML file build successfully!!!")


if __name__ == "__main__":
    setup()
