from src import parser, processor

def main():
    print("Starting spendwise-local pipeline...")
    df = parser.parse_all_pdfs()
    if not df.empty:
        processor.process_and_store(df)

    print("[PIPELINE COMPLETE]")

if __name__ == "__main__":
    main()
