import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, f1_score, classification_report

CSV_PATH = "dataset/AirQualityUCI.csv"
MODEL_PATH = "model.pkl"

TARGET = "CO(GT)"

FEATURES = [
    "PT08.S1(CO)",
    "NMHC(GT)",
    "C6H6(GT)",
    "PT08.S2(NMHC)",
    "NOx(GT)",
    "PT08.S3(NOx)",
    "NO2(GT)",
    "PT08.S4(NO2)",
    "PT08.S5(O3)",
    "T",
    "RH",
    "AH",
]

def load_and_clean(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        decimal=",", 
    )

    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    df = df.drop(columns=["Date", "Time"])

    df = df.replace(-200, pd.NA).dropna()

    return df


def bin_co(values: pd.Series) -> pd.Series:
    return values.apply(bin_co_label)

def bin_co_label(co):
    if co < 2.0:
        return "Low"
    elif co < 4.0:
        return "Medium"
    else:
        return "High"

def main() -> None:
    print("Initialize...")
    df = load_and_clean(CSV_PATH)
    print(f" cleaned rows: {len(df)}")

    X = df[FEATURES]
    y = bin_co(df[TARGET])

    print(f"  class distribution:\n{y.value_counts().to_string()}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000)),
    ])

    print("Train...")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average="weighted")

    print("\n=== Report ===")
    print(f"  Accuracy:        {acc:.4f}")
    print(f"  F1 (weighted):   {f1:.4f}")
    print("\nFull report:")
    print(classification_report(y_test, y_pred))

    joblib.dump({"model": model, "features": FEATURES}, MODEL_PATH)
    print(f"\nSaved -> {MODEL_PATH}")


if __name__ == "__main__":
    main()