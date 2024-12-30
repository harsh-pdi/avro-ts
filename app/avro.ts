import { createFileDecoder, createFileEncoder, Schema, streams } from "avsc";
import { createWriteStream } from 'fs';
import { Message } from "./libs/types";

export const createAvroFile = (data: Array<Message> = [], filePath: string, schema: Schema) => {
    return new Promise((resolve, reject) => {
        const avroFile = createWriteStream(filePath);
        const encoder = new streams.BlockEncoder(schema);
        encoder.pipe(avroFile);
        data.forEach(record => encoder.write(record));
        encoder.end();
        avroFile.on('finish', () => resolve(filePath));
        avroFile.on('error', () => reject());
    });
}

export const updateAvroFile = (data: Array<Message> = [], filePath: string, schema: Schema) => {
    return new Promise(async (resolve, reject) => {
        const messages: any = await readAvroFile(filePath);
        const fileEncoder = createFileEncoder(filePath, schema);
        [...messages, ...data].forEach(record => fileEncoder.write(record));
        fileEncoder.end();
        fileEncoder.on('finish', () => resolve(filePath));
        fileEncoder.on('error', () => reject());
    });
}

export const readAvroFile = (filePath: string) => {
    return new Promise((resolve, reject) => {
        const avroFile = createFileDecoder(filePath);
        const messages: Array<Message> = [];
        avroFile.on('data', (data: Message) => {
            messages.push(JSON.parse(data.toString()));
            resolve(messages);
        });
        avroFile.on('error', () => reject());
    });
}
