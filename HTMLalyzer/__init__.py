def setup() -> None:
    import base64
    import os

    from jinja2 import Environment, FileSystemLoader, environment

    import HTMLalyzer
    import logpyle

    print("Building HTML file for HTMLalyzer!")

    with open("HTMLalyzer/logpyle-2023.2.3-py2.py3-none-any.whl", "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        logpyle_whl_file_str = data.decode("utf-8")

    with open("HTMLalyzer/pymbolic-2022.2-py3-none-any.whl", "rb") as f:
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
            )
    main_css = open(html_path+"/main.css", "r").read()
    main_js = open(html_path+"/main.js", "r").read()

    # os.system("python3 -m build --wheel -o HTMLalyzer/")

    content = template.render(
            cssFile=main_css,
            pythonFile=main_py,
            jsFile=main_js,
            )

    filename = "web-interface.html"
    with open(html_path+"/"+filename, mode="w", encoding="utf-8") as message:
        message.write(content)

    print("HTML file build successfully!!!")


if __name__ == "__main__":
    setup()
