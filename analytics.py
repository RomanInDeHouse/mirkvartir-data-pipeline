import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Загружаем новый сгенерированный датасет
df = pd.read_csv("mirkvartir_data.csv", sep=";")

# Выводим базовую статистику по квадратам
print("=== Аналитика стоимости квадратного метра в Москве ===")
print(f"Минимальная цена за м²: {df['price_per_meter'].min():,} ₽".replace(',', ' '))
print(f"Максимальная цена за м²: {df['price_per_meter'].max():,} ₽".replace(',', ' '))
print(f"Средняя цена за м²:      {int(df['price_per_meter'].mean()):,} ₽".replace(',', ' '))
print(f"Медианная цена за м²:    {int(df['price_per_meter'].median()):,} ₽".replace(',', ' '))

print("\n=== Аналитика площадей квартир ===")
print(f"Средняя площадь жилья в выдаче: {df['calculated_area'].mean():.1f} м²")
print(f"Медианная площадь жилья:        {df['calculated_area'].median():.1f} м²")


# Отсекаем выбросы (дороже 100 млн и площадью больше 200 метров) для точности и игнора треша
filtered_df = df[(df['total_price'] <= 100_000_000) & (df['calculated_area'] <= 200)].copy()

# СРАЗУ создаем колонку в миллионах, чтобы она была доступна для всех графиков ниже
filtered_df['total_price_mln'] = filtered_df['total_price'] / 1_000_000

plt.figure(figsize=(10, 6))


sns.scatterplot(data=filtered_df, x='calculated_area', y='total_price_mln',
                alpha=0.6, color='teal', label='Квартиры')

# Добавляем линию тренда
sns.regplot(data=filtered_df, x='calculated_area', y='total_price_mln',
            scatter=False, color='red', line_kws={"linewidth": 2}, label='Тренд рынка')

# Оформление осей и сетки
plt.title('Зависимость стоимости квартиры от её площади в Москве', fontsize=14, fontweight='bold')
plt.xlabel('Площадь квартиры (м²)', fontsize=12)
plt.ylabel('Цена квартиры (в млн ₽)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend()


plt.show()