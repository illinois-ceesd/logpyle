def setup() -> None:
    import base64
    import os

    from jinja2 import Environment, FileSystemLoader, environment

    import logpyle.HTMLalyzer
    from logpyle import HTMLalyzer

    html_path = os.path.dirname(logpyle.HTMLalyzer.__file__)

    print("Building HTML file for HTMLalyzer!")

    # gather whl filenames
    html_files = os.listdir(html_path)
    logpyle_whl_file_name = None
    pymbolic_whl_file_name = None
    for s in html_files:
        if s.startswith("logpyle"):
            logpyle_whl_file_name = s
        if s.startswith("pymbolic"):
            pymbolic_whl_file_name = s
    assert logpyle_whl_file_name, "logpyle .whl file not found"
    assert pymbolic_whl_file_name, "pymbolic .whl file not found"

    # get logpyle whl
    with open(html_path+"/"+logpyle_whl_file_name, "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        logpyle_whl_file_str = data.decode("utf-8")
    # get pymbolic whl
    with open(html_path+"/"+pymbolic_whl_file_name, "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        pymbolic_whl_file_str = data.decode("utf-8")

    html_path = os.path.dirname(HTMLalyzer.__file__)
    enviroment = Environment(loader=FileSystemLoader(html_path+"/templates/"))
    template = enviroment.get_template("index.html")

    new_file_html = open(html_path+"/templates/newFile.html", "r").read()
    main_py = open(html_path+"/main.py", "r").read()
    main_py_env: environment.Template = Environment().from_string(main_py)
    main_py = main_py_env.render(
            new_file_html=new_file_html,
            logpyle_whl_file_str=logpyle_whl_file_str,
            pymbolic_whl_file_str=pymbolic_whl_file_str,
            logpyle_whl_file_name=logpyle_whl_file_name,
            pymbolic_whl_file_name=pymbolic_whl_file_name,
            )
    main_css = open(html_path+"/main.css", "r").read()
    main_js = open(html_path+"/main.js", "r").read()

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
