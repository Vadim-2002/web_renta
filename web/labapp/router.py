# Подключаем объект приложения Flask из __init__.py
from labapp import app
# Подключаем библиотеку для "рендеринга" html-шаблонов из папки templates
from flask import render_template, make_response, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash

import labapp.webservice as webservice   # подключаем модуль с реализацией бизнес-логики обработки запросов
import smtplib

"""
    Модуль регистрации обработчиков маршрутов, т.е. здесь реализуется обработка запросов
    при переходе пользователя на определенные адреса веб-приложения
"""

value_category = None
value_sort = ""
str_of_columns = "bedroom, localities.name, property_types.name, price, area, cities.name, categories.category"
str_join = "JOIN localities on localities.id = rent_real_estate.locality JOIN property_types on property_types.id = rent_real_estate.property_type JOIN cities on cities.id = rent_real_estate.city JOIN categories on categories.id = rent_real_estate.category"
str_col_names = 'bedroom, locality, property_type, price, area, city, category'
num_page = 0

col_name_pay = []
name_user_lk = "Вход"
email_user_lk = ""
previous_page = ""


def soring_by_price():
    global value_sort

    if request.form.get('sort') == 'price_decrease':
        value_sort = "DESC"
    elif request.form.get('sort') == 'price_increase':
        value_sort = "ASC"


def price_category():
    global value_category

    if request.form.get('price_category') == 'min_price':
        value_category = 1
    elif request.form.get('price_category') == 'average_price':
        value_category = 2
    elif request.form.get('price_category') == 'max_price':
        value_category = 3


def column_selection():
    global str_of_columns
    global str_join
    global str_col_names

    if request.form.get('but_cat') == 'Выбрать':
        str_of_columns = ""
        str_col_names = ""
        str_join = ""

        if request.form.get('cb_cat_1') == 'Bedroom':
            str_of_columns += 'bedroom, '
            str_col_names += 'bedroom, '
        if request.form.get('cb_cat_2') == 'Locality':
            str_of_columns += 'localities.name, '
            str_col_names += 'locality, '
            str_join += 'JOIN localities on localities.id = rent_real_estate.locality '

        if request.form.get('cb_cat_3') == 'Property_type':
            str_of_columns += 'property_types.name, '
            str_col_names += 'property, '
            str_join += 'JOIN property_types on property_types.id = rent_real_estate.property_type '
        str_of_columns += 'price, '
        str_col_names += 'price, '
        if request.form.get('cb_cat_4') == 'Area':
            str_of_columns += 'area, '
            str_col_names += 'area, '
        if request.form.get('cb_cat_5') == 'City':
            str_of_columns += 'cities.name, '
            str_col_names += 'city, '
            str_join += 'JOIN cities on cities.id = rent_real_estate.city '
        str_of_columns += 'categories.category'
        str_col_names += 'category, '
        str_join += 'JOIN categories on categories.id = rent_real_estate.category'


def changing_the_page():
    global num_page

    if request.form.get('page_last') == 'Предыдущая страница':
        if num_page > 0:
            num_page -= 1

    if request.form.get('page_next') == 'Следующая страница':
        if num_page < 3860:
            num_page += 1


def registering_new_user():
    """Запись данных нового пользователя в БД"""
    global name_user_lk
    global email_user_lk

    name = request.form.get('name_user')
    email = request.form.get('email')
    password = request.form.get('pswd')
    hash_pswd = generate_password_hash(password)
    webservice.add_new_user(name, email, hash_pswd)
    name_user_lk = name
    email_user_lk = email
    message = "Hello! Thank you for registering on the Real Estate Rent website in India! We hope you will like it here! We wish you all the best! See you in India!"
    sending_message_user(f"{email}", message)

    return True


def user_authentication():
    """Вход пользователя на сайт"""
    global name_user_lk
    global email_user_lk

    email = request.form.get('email')
    password = request.form.get('pswd')
    data_user = webservice.get_data_user(email)
    save_name = (data_user[0])[0]
    save_password = (data_user[0])[1]
    save_email = (data_user[0])[2]

    if email == save_email:
        if check_password_hash(save_password, password):
            name_user_lk = save_name
            email_user_lk = save_email
            return True
    return False


def sending_message_user(email, message):
    smtp_obj = smtplib.SMTP('smtp.mail.ru', 587)
    smtp_obj.starttls()
    smtp_obj.login('ufa.gentlemen@mail.ru', '3nZKWwtkS8vsi3C6qHqr')
    smtp_obj.sendmail('ufa.gentlemen@mail.ru', f'{email}', f'{message}')
    smtp_obj.quit()


@app.route('/index', methods=['GET', 'POST'])
def index():
    if name_user_lk == "Вход":
        return registration()

    """ Обработка запроса к индексной странице """
    processed_files = webservice.get_sorting_files(str_of_columns, value_category, value_sort, str_join, num_page)

    global previous_page
    previous_page = "/index"

    if request.method == 'POST':
        price_category()
        column_selection()
        soring_by_price()
        changing_the_page()

        processed_files = webservice.get_sorting_files(str_of_columns, value_category, value_sort, str_join, num_page)

    return render_template('dist/www/index.html',
                           title='Home',
                           navmenu=webservice.navmenu,
                           processed_files=processed_files,
                           num_page=num_page,
                           name_user_lk=name_user_lk,
                           name_of_columns=str_col_names.split(","))


@app.route('/contact', methods=['GET'])
def contact():
    """ Обработка запроса к странице contact.html """
    global previous_page
    previous_page = "/contact"

    return render_template('dist/www/contacts.html',
                           title='Contacts',
                           name_user_lk=name_user_lk,
                           navmenu=webservice.navmenu)


@app.route('/about', methods=['GET'])
def about():
    """ Обработка запроса к странице about.html """
    global previous_page
    previous_page = "/about"

    return render_template('dist/www/about.html',
                           title='About project',
                           name_user_lk=name_user_lk,
                           navmenu=webservice.navmenu)


@app.route('/regist', methods=['GET', 'POST'])
def registration():
    global name_user_lk
    global previous_page
    previous_page = "/regist"

    if name_user_lk != "Вход":
        return personal()

    if request.method == 'POST':
        if request.form.get('new_user') == 'True':
            registering_new_user()
            return registration()

        if request.form.get('input_user') == 'True':
            user_authentication()
            return registration()

    return render_template('dist/www/registration.html',
                           title='Login',
                           name_user_lk=name_user_lk,
                           navmenu=webservice.navmenu)


@app.route('/personal', methods=['GET', 'POST'])
def personal():
    global name_user_lk
    global email_user_lk
    global previous_page
    previous_page = "/personal"

    if request.method == 'POST':
        if request.form.get('exit') == 'True':
            name_user_lk = "Вход"
            email_user_lk = ""
            return registration()

        if request.form.get('delete') == 'True':
            print("delete")
            print(email_user_lk)
            webservice.delete_user(email_user_lk)
            name_user_lk = "Вход"
            email_user_lk = ""
            return registration()

    return render_template('dist/www/lk.html',
                           title='Personal account',
                           name_user_lk=name_user_lk,
                           email_user_lk=email_user_lk,
                           navmenu=webservice.navmenu)


@app.route('/sale', methods=['GET', 'POST'])
def sale():
    if name_user_lk == "Вход":
        return registration()

    global previous_page
    previous_page = "/sale"

    processed_files = webservice.get_sorting_files(str_of_columns, value_category, value_sort, str_join, num_page)

    if request.method == 'POST':
        price_category()
        column_selection()
        soring_by_price()
        changing_the_page()

        processed_files = webservice.get_sorting_files(str_of_columns, value_category, value_sort, str_join, num_page)

    return render_template('dist/www/sale.html',
                           title='Sale',
                           navmenu=webservice.navmenu,
                           processed_files=processed_files,
                           num_page=num_page,
                           name_user_lk=name_user_lk,
                           name_of_columns=str_col_names.split(","))


@app.route('/pay', methods=['GET', 'POST'])
def pay():
    if name_user_lk == "Вход":
        return registration()

    global col_name_pay

    if request.method == 'POST':
        col_name_pay.append(request.form.get('col_1'))
        col_name_pay.append(request.form.get('col_2'))
        col_name_pay.append(request.form.get('col_3'))
        col_name_pay.append(request.form.get('col_4'))
        col_name_pay.append(request.form.get('col_5'))
        col_name_pay.append(request.form.get('col_6'))
        col_name_pay.append(request.form.get('col_7'))

        if request.form.get('btn_ofrm') == 'True':
            message = f"Congratulations! You have issued a real estate lease! (BEDROOM: {col_name_pay[0]}, LOCALITY: {col_name_pay[1]}, PROPERTY TYPE: {col_name_pay[2]}, PRICE: {col_name_pay[3]}, DISTRICT: {col_name_pay[4]}, CITY: {col_name_pay[5]})"
            sending_message_user(email_user_lk, message)

    return render_template('dist/www/pay/dist/index.html',
                           title='Pay',
                           col_name_pay=col_name_pay,
                           name_user_lk=name_user_lk,
                           navmenu=webservice.navmenu,
                           prev_page=previous_page)


@app.route('/', methods=['GET'])
def homepage():
    return render_template('dist/index.html',
                           title='Renta',
                           name_user_lk=name_user_lk,
                           navmenu=webservice.navmenu)


@app.route('/notfound', methods=['GET'])
def not_found_html():
    """ Возврат html-страницы с кодом 404 (Не найдено) """
    return render_template('404.html', title='404', err={'error': 'Not found', 'code': 404})


def bad_request():
    """ Формирование json-ответа с ошибкой 400 протокола HTTP (Неверный запрос) """
    return make_response(jsonify({'message': 'Bad request !'}), 400)
